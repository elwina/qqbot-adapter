// Package serve exposes the consumer-facing event stream on the WS port.
//
//	WS  any path            -> hello{last_seq,name?} -> replay -> ready -> live -> ping
//	GET /healthz            -> health snapshot
//	GET /v1/events?since=&limit= -> HTTP replay, same payload shape as WS events
package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"nhooyr.io/websocket"

	"qqbot-adapter/internal/config"
	"qqbot-adapter/internal/health"
	"qqbot-adapter/internal/spool"
)

type Server struct {
	spool  *spool.Spool
	health *health.State
	batch  int
}

func New(sp *spool.Spool, h *health.State, cfg *config.Config) *Server {
	return &Server{spool: sp, health: h, batch: cfg.ReplayBatch}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/v1/events", s.handleEvents)
	mux.HandleFunc("/", s.handleWS) // v1 accepted any upgrade path
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.health.Snapshot())
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 5000 {
		limit = s.batch
	}
	evs, err := s.spool.ReadSince(since, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	items := make([]json.RawMessage, 0, len(evs))
	var last int64 = since
	for _, e := range evs {
		wire, _ := e.WireJSON()
		env, _ := marshalNoEscape(map[string]any{
			"type": "event", "seq": e.Seq, "event": json.RawMessage(wire),
		})
		items = append(items, env)
		if e.Seq > last {
			last = e.Seq
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events":   items,
		"last_seq": last,
	})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !isUpgrade(r) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		log.Printf("[serve] WS accept 失败: %v", err)
		return
	}
	defer ws.CloseNow()
	s.serveConn(ws, r)
}

func isUpgrade(r *http.Request) bool {
	return r.Header.Get("Upgrade") != ""
}

func (s *Server) serveConn(ws *websocket.Conn, r *http.Request) {
	wsend := func(v any) bool {
		wctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		b, err := marshalNoEscape(v)
		if err != nil {
			return false
		}
		return ws.Write(wctx, websocket.MessageText, b) == nil
	}

	// hello within 30s
	rctx, rcancel := context.WithTimeout(r.Context(), 30*time.Second)
	_, raw, err := ws.Read(rctx)
	rcancel()
	var hello struct {
		LastSeq int64  `json:"last_seq"`
		Name    string `json:"name"`
	}
	if err != nil {
		return // v1: 30s 内未发 hello 即断开
	}
	json.Unmarshal(raw, &hello)
	// After the hello is consumed, discard further client frames; ctx ends on disconnect.
	ctx := ws.CloseRead(r.Context())
	ch := s.spool.Subscribe()
	defer s.spool.Unsubscribe(ch)
	s.spool.TouchConsumer(hello.Name, hello.LastSeq)
	log.Printf("[serve] 订阅 last_seq=%d name=%q remote=%s", hello.LastSeq, hello.Name, r.RemoteAddr)

	// replay
	var lastSeq = hello.LastSeq
	for {
		evs, err := s.spool.ReadSince(lastSeq, s.batch)
		if err != nil {
			log.Printf("[serve] 重放读取失败: %v", err)
			return
		}
		if len(evs) == 0 {
			break
		}
		for _, e := range evs {
			wire, _ := e.WireJSON()
			if !wsend(json.RawMessage(buildEnvelope(e.Seq, wire))) {
				return
			}
			lastSeq = e.Seq
			s.spool.TouchConsumer(hello.Name, lastSeq)
		}
		if len(evs) < s.batch {
			break
		}
	}
	if !wsend(map[string]any{"type": "ready"}) {
		return
	}

	// live push; skip events already covered by replay (dup window), idle 45s -> ping
	idle := time.NewTimer(45 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			if e.Seq <= lastSeq {
				continue
			}
			lastSeq = e.Seq
			wire, _ := e.WireJSON()
			if !wsend(json.RawMessage(buildEnvelope(e.Seq, wire))) {
				return
			}
			s.spool.TouchConsumer(hello.Name, e.Seq)
			resetTimer(idle, 45*time.Second)
		case <-idle.C:
			if !wsend(map[string]any{"type": "ping"}) {
				return
			}
			resetTimer(idle, 45*time.Second)
		}
	}
}

// buildEnvelope renders {"type":"event","seq":N,"event":{...}} without re-encoding.
func buildEnvelope(seq int64, wire []byte) []byte {
	out := make([]byte, 0, len(wire)+40)
	out = append(out, `{"type":"event","seq":`...)
	out = strconv.AppendInt(out, seq, 10)
	out = append(out, `,"event":`...)
	out = append(out, wire...)
	out = append(out, '}')
	return out
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func marshalNoEscape(v any) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	return b[:len(b)-1], nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}
