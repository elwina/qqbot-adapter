// Package qqgw maintains the outbound WSS connection to QQ's gateway.
//
// Port of v1 gateway.py: op10 hello -> identify(op2)/resume(op6) ->
// op0 dispatch persists to spool, op11 heartbeat ack, op7 reconnect-resume,
// op9 invalid session; session_start_limit guardrail; intent close-code hints.
package qqgw

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"

	"qqbot-adapter/internal/health"
	"qqbot-adapter/internal/spool"
	"qqbot-adapter/internal/token"
)

// Control frames are dispatches too but must not pollute business replay.
var controlTypes = map[string]bool{"READY": true, "RESUMED": true}

// closeCodeHints mirror v1 CLOSE_CODE_HINTS.
var closeCodeHints = map[int]string{
	4013: "invalid intents：intent 取值非法",
	4014: "disallowed intents：机器人无权订阅所请求的 intent",
	4914: "intent 未在开放平台开启：请到 q.qq.com 开发配置中打开对应 intent 开关",
}

const fallbackWSURL = "wss://api.sgroup.qq.com/websocket"

type Gateway struct {
	apiBase    string
	gatewayURL string // test hook: skip /gateway/bot
	intents    int64
	spool      *spool.Spool
	tok        *token.Provider
	health     *health.State
	client     *http.Client

	mu        sync.Mutex // guards seq/sessionID/resuming
	seq       int64
	hasSeq    bool
	sessionID string
	resuming  bool
}

func New(apiBase, gatewayURL string, intents int64, sp *spool.Spool, tok *token.Provider, h *health.State) *Gateway {
	return &Gateway{
		apiBase:    apiBase,
		gatewayURL: gatewayURL,
		intents:    intents,
		spool:      sp,
		tok:        tok,
		health:     h,
		client:     &http.Client{Timeout: 15 * time.Second},
	}
}

type frame struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  *int64          `json:"s"`
	T  string          `json:"t"`
}

// Run is the reconnect loop; it returns only when ctx is cancelled.
func (g *Gateway) Run(ctx context.Context) {
	attempt := 0
	for ctx.Err() == nil {
		url, remaining := g.fetchGateway(ctx)
		if remaining >= 0 && remaining < 5 {
			log.Printf("[qqgw] session_start_limit 剩余 %d 次，暂停 5 分钟", remaining)
			if !sleepCtx(ctx, 5*time.Minute) {
				return
			}
			continue
		}
		closeCode := g.session(ctx, url)
		g.health.SetOffline(closeCode, "")
		if ctx.Err() != nil {
			return
		}
		var delay time.Duration
		if _, bad := closeCodeHints[closeCode]; bad {
			delay = 120 * time.Second
		} else {
			attempt++
			delay = time.Duration(min(60, 1<<min(attempt, 6)))*time.Second + time.Duration(rand.Float64()*1000)*time.Millisecond
		}
		log.Printf("[qqgw] %.1f 秒后重连", delay.Seconds())
		if !sleepCtx(ctx, delay) {
			return
		}
	}
}

// session runs one WS connection until it closes; returns the close code.
func (g *Gateway) session(ctx context.Context, url string) int {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ws, _, err := websocket.Dial(cctx, url, nil)
	if err != nil {
		log.Printf("[qqgw] 连接 %s 失败: %v", url, err)
		return 0
	}
	ws.SetReadLimit(4 << 20)
	log.Printf("[qqgw] WS 已连接 %s", url)

	writeMu := sync.Mutex{}
	send := func(v any) error {
		b, err := marshalNoEscape(v)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		wctx, wc := context.WithTimeout(cctx, 10*time.Second)
		defer wc()
		return ws.Write(wctx, websocket.MessageText, b)
	}

	hbStop := make(chan struct{})
	var hbWg sync.WaitGroup
	var hbStarted atomic.Bool
	startHeartbeat := func(interval time.Duration) {
		if !hbStarted.CompareAndSwap(false, true) {
			return
		}
		hbWg.Add(1)
		go func() {
			defer hbWg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-hbStop:
					return
				case <-cctx.Done():
					return
				case <-t.C:
					g.mu.Lock()
					var d any
					if g.hasSeq {
						d = g.seq
					}
					g.mu.Unlock()
					if err := send(map[string]any{"op": 1, "d": d}); err != nil {
						log.Printf("[qqgw] 心跳发送失败: %v", err)
						return
					}
				}
			}
		}()
	}
	defer func() {
		close(hbStop)
		hbWg.Wait()
		ws.Close(websocket.StatusNormalClosure, "bye")
	}()

	closeCode := 0
	for {
		_, data, err := ws.Read(cctx)
		if err != nil {
			closeCode = int(websocket.CloseStatus(err))
			reason := ""
			var ce websocket.CloseError
			if asCloseError(err, &ce) {
				reason = ce.Reason
			}
			if hint, ok := closeCodeHints[closeCode]; ok {
				log.Printf("[qqgw] WS 关闭 code=%d reason=%q；%s；intents=%d", closeCode, reason, hint, g.intents)
			} else {
				log.Printf("[qqgw] WS 读取结束: %v", err)
			}
			return closeCode
		}
		var msg frame
		if json.Unmarshal(data, &msg) != nil {
			log.Printf("[qqgw] 非 JSON 消息，忽略")
			continue
		}
		switch msg.Op {
		case 10: // hello
			var d struct {
				HeartbeatInterval int64 `json:"heartbeat_interval"`
			}
			json.Unmarshal(msg.D, &d)
			iv := time.Duration(d.HeartbeatInterval) * time.Millisecond
			if iv <= 0 {
				iv = 41250 * time.Millisecond
			}
			g.mu.Lock()
			resume := g.resuming && g.sessionID != ""
			g.mu.Unlock()
			if resume {
				err = g.sendResume(send)
			} else {
				err = g.sendIdentify(send)
			}
			if err != nil {
				log.Printf("[qqgw] identify/resume 发送失败: %v", err)
				return 0
			}
			startHeartbeat(iv)
		case 11: // heartbeat ack
			g.health.NoteHBAck()
		case 0: // dispatch
			g.onDispatch(msg)
		case 7: // reconnect -> resume
			log.Printf("[qqgw] 收到 op7，重连 resume")
			g.mu.Lock()
			g.resuming = true
			g.mu.Unlock()
			return 0
		case 9: // invalid session
			resumable := string(msg.D) == "true"
			log.Printf("[qqgw] 收到 op9 invalid session（resumable=%v）", resumable)
			g.mu.Lock()
			if resumable && g.sessionID != "" {
				g.resuming = true
			} else {
				g.resuming, g.sessionID, g.hasSeq = false, "", false
			}
			g.mu.Unlock()
			if !resumable {
				time.Sleep(time.Duration(1+rand.Float64()*4) * time.Second)
			}
			return 0
		default:
			log.Printf("[qqgw] 未知 op=%d，忽略", msg.Op)
		}
	}
}

func (g *Gateway) onDispatch(msg frame) {
	g.mu.Lock()
	if msg.S != nil {
		g.seq, g.hasSeq = *msg.S, true
	}
	curSeq, sid := g.seq, g.sessionID
	g.mu.Unlock()
	g.health.SetSeq(curSeq)
	nowMs := time.Now().UnixMilli()
	switch {
	case msg.T == "READY":
		var d struct {
			SessionID string `json:"session_id"`
		}
		json.Unmarshal(msg.D, &d)
		g.mu.Lock()
		g.sessionID = d.SessionID
		g.resuming = false
		g.mu.Unlock()
		g.health.SetOnline(d.SessionID, curSeq)
		g.spool.InsertControl(nowMs, msg.T, msg.S, msg.D, nil)
		log.Printf("[qqgw] identify 被接受（READY），session_id=%s", d.SessionID)
	case msg.T == "RESUMED":
		// v1 keeps resuming=true after RESUMED so later drops keep resuming.
		g.health.SetOnline(sid, curSeq)
		g.spool.InsertControl(nowMs, msg.T, msg.S, msg.D, nil)
		log.Printf("[qqgw] resume 成功（RESUMED）")
	case controlTypes[msg.T]:
		g.spool.InsertControl(nowMs, msg.T, msg.S, msg.D, nil)
	default:
		if _, err := g.spool.InsertEvent(nowMs, msg.Op, msg.T, msg.S, msg.D); err != nil {
			log.Printf("[qqgw] 事件入库失败 t=%s s=%v: %v", msg.T, msg.S, err)
		}
		g.health.NoteDispatch()
	}
}

func (g *Gateway) sendIdentify(send func(any) error) error {
	tok, err := g.tok.Token()
	if err != nil {
		return fmt.Errorf("取 token: %w", err)
	}
	log.Printf("[qqgw] 已发送 identify（intents=%d）", g.intents)
	return send(map[string]any{
		"op": 2,
		"d": map[string]any{
			"token":   "QQBot " + tok,
			"intents": g.intents,
			"shard":   [2]int{0, 1},
			"properties": map[string]string{
				"$os":      "linux",
				"$browser": "qqbot-adapter/2.0",
				"$device":  "qqbot-adapter",
			},
		},
	})
}

func (g *Gateway) sendResume(send func(any) error) error {
	tok, err := g.tok.Token()
	if err != nil {
		return fmt.Errorf("取 token: %w", err)
	}
	g.mu.Lock()
	sid, seq := g.sessionID, g.seq
	g.mu.Unlock()
	log.Printf("[qqgw] 已发送 resume（session_id=%s seq=%d）", sid, seq)
	return send(map[string]any{
		"op": 6,
		"d":  map[string]any{"token": "QQBot " + tok, "session_id": sid, "seq": seq},
	})
}

// fetchGateway GETs {apiBase}/gateway/bot; returns (url, remaining) where
// remaining<0 means unknown (fallback path also uses it).
func (g *Gateway) fetchGateway(ctx context.Context) (string, int) {
	if g.gatewayURL != "" {
		return g.gatewayURL, -1
	}
	tok, err := g.tok.Token()
	if err != nil {
		log.Printf("[qqgw] 取 token 失败(%v)，使用回退地址", err)
		return fallbackWSURL, -1
	}
	req, err := http.NewRequestWithContext(ctx, "GET", g.apiBase+"/gateway/bot", nil)
	if err != nil {
		return fallbackWSURL, -1
	}
	req.Header.Set("Authorization", "QQBot "+tok)
	resp, err := g.client.Do(req)
	if err != nil {
		log.Printf("[qqgw] 取 gateway 地址失败(%v)，使用回退地址", err)
		return fallbackWSURL, -1
	}
	defer resp.Body.Close()
	var data struct {
		URL                string `json:"url"`
		SessionStartLimit struct {
			Remaining int `json:"remaining"`
		} `json:"session_start_limit"`
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 || json.Unmarshal(body, &data) != nil || data.URL == "" {
		return fallbackWSURL, -1
	}
	return data.URL, data.SessionStartLimit.Remaining
}

func marshalNoEscape(v any) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	return b[:len(b)-1], nil // strip trailing newline
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func asCloseError(err error, out *websocket.CloseError) bool {
	if e, ok := err.(websocket.CloseError); ok {
		*out = e
		return true
	}
	return false
}
