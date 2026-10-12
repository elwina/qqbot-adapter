// Package outbox retries queued outbound QQ API calls with exponential backoff.
//
// Retry: 5s doubling to a 5min cap; entries older than maxAge are marked failed.
package outbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"qqbot-adapter/internal/spool"
	"qqbot-adapter/internal/token"
)

const (
	baseDelay = 5 * time.Second
	maxDelay  = 5 * time.Minute
	pollEvery = 2 * time.Second
	batchSize = 50
)

// Sender executes one outbound call against QQ. Shared by relay and worker.
type Sender struct {
	apiBase string
	tok     *token.Provider
	client  *http.Client
}

func NewSender(apiBase string, tok *token.Provider) *Sender {
	return &Sender{apiBase: apiBase, tok: tok, client: &http.Client{Timeout: 25 * time.Second}}
}

// Do performs the upstream call. Returns (respStatus, respBody, retriable, err).
// Retriable means network failure or HTTP 5xx — eligible for outbox retry.
func (s *Sender) Do(method, path string, body []byte, contentType string) (int, []byte, bool, error) {
	tok, err := s.tok.Token()
	if err != nil {
		return 0, nil, true, fmt.Errorf("取 token 失败: %w", err)
	}
	url := s.apiBase + path
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, false, err
	}
	req.Header.Set("Authorization", "QQBot "+tok)
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, true, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp.StatusCode, rb, resp.StatusCode >= 500, nil
}

// Worker drains due outbox entries until ctx is cancelled.
type Worker struct {
	spool     *spool.Spool
	sender    *Sender
	maxAge    time.Duration
	pollEvery time.Duration
}

func NewWorker(sp *spool.Spool, sender *Sender, maxAge time.Duration) *Worker {
	return &Worker{spool: sp, sender: sender, maxAge: maxAge, pollEvery: pollEvery}
}

// SetPollEvery overrides the drain interval (tests).
func (w *Worker) SetPollEvery(d time.Duration) { w.pollEvery = d }

func (w *Worker) Run(ctx context.Context) {
	t := time.NewTicker(w.pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.drain(ctx)
		}
	}
}

func (w *Worker) drain(ctx context.Context) {
	now := time.Now().UnixMilli()
	entries, err := w.spool.DueOutbox(now, batchSize)
	if err != nil {
		log.Printf("[outbox] 取待重试队列失败: %v", err)
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if w.maxAge > 0 && now-e.CreatedMs > w.maxAge.Milliseconds() {
			w.spool.MarkOutboxFailed(e.ID, "超过最大保留时长")
			log.Printf("[outbox] #%d 超过 %v 未成功，标记 failed", e.ID, w.maxAge)
			continue
		}
		status, respBody, retriable, err := w.sender.Do(e.Method, e.Path, e.Body, e.ContentType)
		switch {
		case err == nil && !retriable:
			w.spool.MarkOutboxSent(e.ID, status, respBody)
			log.Printf("[outbox] #%d 重试成功 HTTP %d", e.ID, status)
		case err == nil:
			// 5xx response: keep retrying, record last response.
			next := backoff(e.Attempts + 1)
			w.spool.MarkOutboxRetry(e.ID, time.Now().Add(next).UnixMilli(),
				fmt.Sprintf("HTTP %d: %s", status, truncate(string(respBody), 300)))
		default:
			next := backoff(e.Attempts + 1)
			w.spool.MarkOutboxRetry(e.ID, time.Now().Add(next).UnixMilli(), truncate(err.Error(), 300))
			log.Printf("[outbox] #%d 重试失败(%v)，%v 后再试", e.ID, err, next)
		}
	}
}

// backoff: 5s * 2^attempts, capped at 5min.
func backoff(attempts int) time.Duration {
	d := baseDelay << min(attempts, 6)
	if d > maxDelay || d < baseDelay {
		return maxDelay
	}
	return d
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
