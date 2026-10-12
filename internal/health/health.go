// Package health holds the adapter's shared runtime state for /healthz.
package health

import (
	"sync"
	"time"
)

type State struct {
	mu sync.Mutex

	StartedAt time.Time

	// QQ gateway side.
	Online      bool
	SessionID   string
	Seq         int64
	Ready       bool
	HBAcks      int64
	Dispatches  int64
	LastEventMs int64
	CloseCode   int
	CloseReason string

	// Subs reports the live WS subscriber count; wired to spool.SubCount.
	Subs func() int
}

func New() *State { return &State{StartedAt: time.Now()} }

func (h *State) SetOnline(sessionID string, seq int64) {
	h.mu.Lock()
	h.Online, h.SessionID, h.Seq, h.Ready = true, sessionID, seq, true
	h.mu.Unlock()
}

func (h *State) SetOffline(code int, reason string) {
	h.mu.Lock()
	h.Online, h.CloseCode, h.CloseReason = false, code, reason
	h.mu.Unlock()
}

func (h *State) SetSeq(seq int64) { h.mu.Lock(); h.Seq = seq; h.mu.Unlock() }

func (h *State) NoteDispatch() {
	h.mu.Lock()
	h.Dispatches++
	h.LastEventMs = time.Now().UnixMilli()
	h.mu.Unlock()
}

func (h *State) NoteHBAck() { h.mu.Lock(); h.HBAcks++; h.mu.Unlock() }

func (h *State) Snapshot() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	subs := 0
	if h.Subs != nil {
		subs = h.Subs()
	}
	return map[string]any{
		"online":         h.Online,
		"ready":          h.Ready,
		"session_id":     h.SessionID,
		"seq":            h.Seq,
		"hb_acks":        h.HBAcks,
		"dispatches":     h.Dispatches,
		"last_event_ts":  h.LastEventMs,
		"subs":           subs,
		"uptime_seconds": int64(time.Since(h.StartedAt).Seconds()),
		"close_code":     h.CloseCode,
		"close_reason":   h.CloseReason,
	}
}
