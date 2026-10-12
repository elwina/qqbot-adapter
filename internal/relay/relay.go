// Package relay exposes the outbound API proxy on the relay port.
//
//	ANY  /v2/*            -> qq api (+outbox fallback: 5xx/network -> 202 queued)
//	POST /v2/*?async=1    -> enqueue immediately, 202 {outbox_id}
//	GET  /v1/outbox/{id}  -> delivery status
//	GET  /healthz         -> health snapshot
package relay

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/elwina/qqbot-adapter/internal/health"
	"github.com/elwina/qqbot-adapter/internal/outbox"
	"github.com/elwina/qqbot-adapter/internal/spool"
)

type Server struct {
	spool  *spool.Spool
	sender *outbox.Sender
	health *health.State
}

func New(sp *spool.Spool, sender *outbox.Sender, h *health.State) *Server {
	return &Server{spool: sp, sender: sender, health: h}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", s.handleV2)
	mux.HandleFunc("/v1/outbox/", s.handleOutboxStatus)
	mux.HandleFunc("/healthz", s.handleHealthz)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.health.Snapshot())
}

func (s *Server) handleOutboxStatus(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/v1/outbox/"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad id"})
		return
	}
	e, err := s.spool.GetOutbox(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if e == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}
	out := map[string]any{
		"id":          e.ID,
		"created_ts":  e.CreatedMs,
		"method":      e.Method,
		"path":        e.Path,
		"status":      e.Status,
		"attempts":    e.Attempts,
		"last_error":  e.LastError,
		"resp_status": e.RespStatus,
	}
	if e.RespBody != nil {
		out["resp_body"] = json.RawMessage(e.RespBody)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleV2(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	contentType := r.Header.Get("Content-Type")
	q := r.URL.Query()
	async := q.Get("async") == "1"
	q.Del("async")
	path := r.URL.EscapedPath()
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}

	if async {
		s.enqueue202(w, r.Method, path, body, contentType)
		return
	}

	status, respBody, retriable, err := s.sender.Do(r.Method, path, body, contentType)
	if err == nil && !retriable {
		if e := s.spool.RecordSent(r.Method, path, body, contentType, status, respBody); e != nil {
			log.Printf("[relay] outbox 审计写入失败: %v", e)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(respBody)
		return
	}
	if err == nil {
		// 5xx from QQ: persist response then queue for retry.
		log.Printf("[relay] %s %s 上游 HTTP %d，入队重试", r.Method, path, status)
	} else {
		log.Printf("[relay] %s %s 中转失败: %v，入队重试", r.Method, path, err)
	}
	s.enqueue202(w, r.Method, path, body, contentType)
}

func (s *Server) enqueue202(w http.ResponseWriter, method, path string, body []byte, contentType string) {
	id, err := s.spool.EnqueueOutbox(method, path, body, contentType)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "入队失败: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"outbox_id": id, "status": "queued"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}
