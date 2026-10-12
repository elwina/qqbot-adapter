// e2e: fake QQ gateway + fake QQ API + real adapter components.
// Covers: token fetch, identify->READY, dispatch persistence, WS replay/ready/live,
// relay sync path, outbox 202 queueing and retry-then-sent.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"nhooyr.io/websocket"

	"github.com/elwina/qqbot-adapter/internal/config"
	"github.com/elwina/qqbot-adapter/internal/health"
	"github.com/elwina/qqbot-adapter/internal/outbox"
	"github.com/elwina/qqbot-adapter/internal/qqgw"
	"github.com/elwina/qqbot-adapter/internal/relay"
	"github.com/elwina/qqbot-adapter/internal/serve"
	"github.com/elwina/qqbot-adapter/internal/spool"
	"github.com/elwina/qqbot-adapter/internal/token"
)

// ---------- fake QQ gateway ----------

type fakeGW struct {
	t        *testing.T
	conn     chan *websocket.Conn
	dispatch chan frame // dispatches to send on current conn
	seq      int64
}

type frame struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

func (f *fakeGW) handler(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer ws.CloseNow()
	f.conn <- ws // slot freed by dropCurrent/close via test
	ws.SetReadLimit(1 << 20)

	send := func(v frame) {
		b, _ := json.Marshal(v)
		ws.Write(r.Context(), websocket.MessageText, b)
	}
	// hello
	send(frame{Op: 10, D: json.RawMessage(`{"heartbeat_interval":400}`)})

	msgs := make(chan frame, 16)
	readErr := make(chan error, 1)
	go func() {
		for {
			_, data, err := ws.Read(r.Context())
			if err != nil {
				readErr <- err
				return
			}
			var m frame
			if json.Unmarshal(data, &m) == nil {
				msgs <- m
			}
		}
	}()

	for {
		select {
		case <-readErr:
			return
		case <-r.Context().Done():
			return
		case d := <-f.dispatch:
			d.Op = 0
			f.seq++
			s := f.seq
			d.S = &s
			send(d)
		case m := <-msgs:
			switch m.Op {
			case 2: // identify
				f.seq++
				s := f.seq
				send(frame{Op: 0, T: "READY", S: &s,
					D: json.RawMessage(`{"session_id":"fake-sess-1","shard":[0,1]}`)})
			case 6: // resume
				f.seq++
				s := f.seq
				send(frame{Op: 0, T: "RESUMED", S: &s, D: json.RawMessage(`{}`)})
			case 1: // heartbeat
				send(frame{Op: 11})
			}
		}
	}
}

func (f *fakeGW) dropCurrent() {
	select {
	case ws := <-f.conn:
		ws.Close(websocket.StatusAbnormalClosure, "boom")
	default:
	}
}

// ---------- fake QQ API ----------

func fakeAPI(t *testing.T, wsURL string, flaky *atomic.Int32) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/app/getAppAccessToken", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"fake-token-abc","expires_in":7200}`)
	})
	mux.HandleFunc("/gateway/bot", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"url":%q,"session_start_limit":{"remaining":100}}`, wsURL)
	})
	mux.HandleFunc("/v2/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"pong":true}`)
	})
	mux.HandleFunc("/v2/flaky", func(w http.ResponseWriter, r *http.Request) {
		if flaky.Add(1) <= 1 {
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprint(w, `{"err":"upstream"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
	})
	return httptest.NewServer(mux)
}

// ---------- test ----------

func startAdapter(t *testing.T, apiBase, gwURL string) (wsPort, relayPort int, sp *spool.Spool, cleanup func()) {
	dir := t.TempDir()
	cfg := &config.Config{
		ListenIP: "127.0.0.1", DBPath: dir + "/state/adapter.db",
		SecretsDir: dir + "/secrets", Intents: 50331648,
		ReplayBatch: 2, // force multi-batch replay
		QQAPIBase:   apiBase, GatewayURL: gwURL,
	}
	writeSecrets(t, cfg.SecretsDir)
	os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755)

	sp, err := spool.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	h := health.New()
	h.Subs = sp.SubCount
	tok := token.New(cfg.QQAPIBase, cfg.SecretsDir, dir+"/state")
	sender := outbox.NewSender(cfg.QQAPIBase, tok)

	ctx, cancel := context.WithCancel(context.Background())
	gw := qqgw.New(cfg.QQAPIBase, cfg.GatewayURL, cfg.Intents, sp, tok, h)
	go gw.Run(ctx)
	obw := outbox.NewWorker(sp, sender, 24*time.Hour)
	obw.SetPollEvery(200 * time.Millisecond)
	go obw.Run(ctx)

	wsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relayLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go http.Serve(wsLn, serve.New(sp, h, cfg).Handler())
	go http.Serve(relayLn, relay.New(sp, sender, h).Handler())

	return wsLn.Addr().(*net.TCPAddr).Port, relayLn.Addr().(*net.TCPAddr).Port, sp, func() {
		cancel()
		wsLn.Close()
		relayLn.Close()
		sp.Close()
	}
}

func writeSecrets(t *testing.T, dir string) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(dir+"/appid.txt", []byte("999"), 0o600)
	os.WriteFile(dir+"/app_secret.txt", []byte("s3cr3t"), 0o600)
}

type wsClient struct {
	t *testing.T
	c *websocket.Conn
}

func dial(t *testing.T, url string, hello string) *wsClient {
	c, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.SetReadLimit(4 << 20)
	if err := c.Write(context.Background(), websocket.MessageText, []byte(hello)); err != nil {
		t.Fatal(err)
	}
	return &wsClient{t, c}
}

func (w *wsClient) next() map[string]any {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, b, err := w.c.Read(ctx)
	if err != nil {
		w.t.Fatalf("ws read: %v", err)
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

func TestEndToEnd(t *testing.T) {
	fg := &fakeGW{t: t, conn: make(chan *websocket.Conn, 1), dispatch: make(chan frame, 8)}
	wsSrv := httptest.NewServer(http.HandlerFunc(fg.handler))
	wsURL := "ws" + wsSrv.URL[len("http"):]
	flaky := &atomic.Int32{}
	api := fakeAPI(t, wsURL, flaky)
	defer api.Close()
	defer wsSrv.Close()

	wsPort, relayPort, sp, cleanup := startAdapter(t, api.URL, "")
	defer cleanup()

	// Pre-seed 3 business events to exercise multi-batch replay.
	for i := 0; i < 3; i++ {
		s := int64(100 + i)
		if _, err := sp.InsertEvent(time.Now().UnixMilli(), 0, "GROUP_MESSAGE_CREATE", &s,
			json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}

	// Consumer connects: replay 3 events then ready.
	cli := dial(t, fmt.Sprintf("ws://127.0.0.1:%d/events", wsPort), `{"last_seq":0,"name":"test-consumer"}`)
	got := 0
	for {
		m := cli.next()
		switch m["type"] {
		case "event":
			got++
			if ev, ok := m["event"].(map[string]any); ok && ev["t"] != "GROUP_MESSAGE_CREATE" {
				t.Fatalf("unexpected event t=%v", ev["t"])
			}
		case "ready":
			goto READY
		}
	}
READY:
	if got != 3 {
		t.Fatalf("replay 条数=%d, want 3", got)
	}

	// Live push: fake gateway dispatches -> consumer sees event.
	fg.dispatch <- frame{T: "GROUP_MESSAGE_CREATE", D: json.RawMessage(`{"live":1}`)}
	deadline := time.Now().Add(15 * time.Second)
	for {
		m := cli.next()
		if time.Now().After(deadline) {
			t.Fatal("live 事件超时")
		}
		if m["type"] == "ping" {
			continue
		}
		if m["type"] == "event" {
			break
		}
	}

	// Drop the QQ-side conn; the gateway must reconnect and re-identify.
	fg.dropCurrent()
	waitFor(t, 15*time.Second, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", wsPort))
		if err != nil {
			return false
		}
		var hz map[string]any
		json.NewDecoder(resp.Body).Decode(&hz)
		resp.Body.Close()
		return hz["online"] == true
	})

	// Relay sync success.
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v2/ping", relayPort),
		"application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("relay status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// Relay 5xx -> 202 + outbox retry until fake API recovers.
	resp, err = http.Post(fmt.Sprintf("http://127.0.0.1:%d/v2/flaky", relayPort),
		"application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var ack struct {
		OutboxID int64 `json:"outbox_id"`
	}
	json.NewDecoder(resp.Body).Decode(&ack)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || ack.OutboxID == 0 {
		t.Fatalf("want 202+outbox_id, got %d %+v", resp.StatusCode, ack)
	}
	waitFor(t, 15*time.Second, func() bool {
		e, err := sp.GetOutbox(ack.OutboxID)
		return err == nil && e != nil && e.Status == "sent"
	})

	// healthz on both ports.
	for _, p := range []int{wsPort, relayPort} {
		resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", p))
		if err != nil {
			t.Fatal(err)
		}
		var hz map[string]any
		json.NewDecoder(resp.Body).Decode(&hz)
		resp.Body.Close()
		if hz["online"] != true {
			t.Fatalf("healthz online=%v", hz["online"])
		}
	}

	// /v1/events?since= returns same stream.
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/events?since=0&limit=100", wsPort))
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Events  []json.RawMessage `json:"events"`
		LastSeq int64             `json:"last_seq"`
	}
	json.NewDecoder(resp.Body).Decode(&page)
	resp.Body.Close()
	if len(page.Events) < 4 || page.LastSeq == 0 {
		t.Fatalf("v1/events: %d events last=%d", len(page.Events), page.LastSeq)
	}
}

func waitFor(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("waitFor 超时")
}
