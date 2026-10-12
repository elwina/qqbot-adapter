// Package spool persists QQ events and outbound API calls in SQLite.
//
// events   — business dispatches; id is the external replay seq.
// control  — READY/RESUMED gateway frames, kept out of the replay stream.
// outbox   — durable outbound requests with retry bookkeeping.
// consumers— optional hello {name} checkpoints.
package spool

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS events(
  id      INTEGER PRIMARY KEY AUTOINCREMENT, -- 永不复用：导入控制帧/retention 都不许挤占消费端位点
  recv_ts INTEGER NOT NULL,
  op      INTEGER NOT NULL,
  t       TEXT    NOT NULL,
  s       INTEGER,
  payload TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS control(
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  recv_ts  INTEGER NOT NULL,
  t        TEXT    NOT NULL,
  s        INTEGER,
  payload  TEXT    NOT NULL,
  orig_seq INTEGER
);
CREATE TABLE IF NOT EXISTS outbox(
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  created_ts    INTEGER NOT NULL,
  method        TEXT    NOT NULL,
  path          TEXT    NOT NULL,
  body          BLOB,
  content_type  TEXT,
  status        TEXT    NOT NULL,
  resp_status   INTEGER,
  resp_body     BLOB,
  attempts      INTEGER NOT NULL DEFAULT 0,
  next_retry_ts INTEGER,
  last_error    TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(status, next_retry_ts);
CREATE TABLE IF NOT EXISTS consumers(
  name       TEXT PRIMARY KEY,
  last_ack   INTEGER,
  updated_ts INTEGER
);
`

// Event is one business dispatch. Seq is the events.id used by replay.
type Event struct {
	Seq     int64
	RecvMs  int64
	Op      int
	T       string
	S       *int64
	Payload json.RawMessage
}

// WireJSON renders the v1-compatible event object {recv_ts,op,t,s,d};
// recv_ts is emitted as float seconds like the Python adapter wrote.
func (e Event) WireJSON() ([]byte, error) {
	buf := make([]byte, 0, len(e.Payload)+96)
	buf = append(buf, `{"recv_ts":`...)
	buf = append(buf, json.Number(fmt.Sprintf("%.3f", float64(e.RecvMs)/1000.0))...)
	buf = append(buf, `,"op":`...)
	buf = append(buf, json.Number(fmt.Sprint(e.Op))...)
	buf = append(buf, `,"t":`...)
	tb, _ := json.Marshal(e.T)
	buf = append(buf, tb...)
	buf = append(buf, `,"s":`...)
	if e.S == nil {
		buf = append(buf, "null"...)
	} else {
		buf = append(buf, json.Number(fmt.Sprint(*e.S))...)
	}
	buf = append(buf, `,"d":`...)
	if len(e.Payload) == 0 {
		buf = append(buf, "null"...)
	} else {
		buf = append(buf, e.Payload...)
	}
	buf = append(buf, '}')
	return buf, nil
}

// OutboxEntry is one durable outbound API call.
type OutboxEntry struct {
	ID          int64
	CreatedMs   int64
	Method      string
	Path        string
	Body        []byte
	ContentType string
	Status      string // queued | sent | failed
	RespStatus  *int
	RespBody    []byte
	Attempts    int
	NextRetryMs int64
	LastError   string
}

type Spool struct {
	db *sql.DB

	mu   sync.Mutex
	subs map[chan Event]struct{}
}

func Open(path string) (*Spool, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	// Serialized access keeps modernc/sqlite simple under WAL.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Spool{db: db, subs: map[chan Event]struct{}{}}, nil
}

func (s *Spool) Close() error { return s.db.Close() }

// DB exposes the underlying handle for bulk operations (import-jsonl).
func (s *Spool) DB() *sql.DB { return s.db }

// ---------- events ----------

// InsertEvent appends a business dispatch and notifies subscribers.
func (s *Spool) InsertEvent(recvMs int64, op int, t string, seq *int64, payload json.RawMessage) (int64, error) {
	res, err := s.db.Exec(`INSERT INTO events(recv_ts,op,t,s,payload) VALUES(?,?,?,?,?)`,
		recvMs, op, t, seq, string(payload))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	s.publish(Event{Seq: id, RecvMs: recvMs, Op: op, T: t, S: seq, Payload: payload})
	return id, nil
}

// InsertEventAt inserts with an explicit id (jsonl line number during import).
func (s *Spool) InsertEventAt(id, recvMs int64, op int, t string, seq *int64, payload json.RawMessage) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO events(id,recv_ts,op,t,s,payload) VALUES(?,?,?,?,?,?)`,
		id, recvMs, op, t, seq, string(payload))
	return err
}

// InsertControl records READY/RESUMED outside the business stream.
func (s *Spool) InsertControl(recvMs int64, t string, seq *int64, payload json.RawMessage, origSeq *int64) error {
	_, err := s.db.Exec(`INSERT INTO control(recv_ts,t,s,payload,orig_seq) VALUES(?,?,?,?,?)`,
		recvMs, t, seq, string(payload), origSeq)
	return err
}

// ReadSince returns up to limit events with id > lastSeq, ascending.
func (s *Spool) ReadSince(lastSeq int64, limit int) ([]Event, error) {
	rows, err := s.db.Query(`SELECT id,recv_ts,op,t,s,payload FROM events WHERE id>? ORDER BY id LIMIT ?`,
		lastSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var pl string
		if err := rows.Scan(&e.Seq, &e.RecvMs, &e.Op, &e.T, &e.S, &pl); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(pl)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Spool) LastSeq() (int64, error) {
	var v sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(id) FROM events`).Scan(&v)
	return v.Int64, err
}

// PinEventSeqFloor raises the events id floor to n so the next row gets id>n.
// Needed after jsonl import: control lines didn't land in events, leaving the
// last line number free for reuse — which collides with consumers' last_seq.
func (s *Spool) PinEventSeqFloor(n int64) error {
	if _, err := s.db.Exec(
		`INSERT INTO sqlite_sequence(name,seq)
		 SELECT 'events', ? WHERE NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name='events')`, n); err != nil {
		return err
	}
	_, err := s.db.Exec(`UPDATE sqlite_sequence SET seq=? WHERE name='events' AND seq<?`, n, n)
	return err
}

// PruneEvents deletes business events older than beforeMs.
func (s *Spool) PruneEvents(beforeMs int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM events WHERE recv_ts<?`, beforeMs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PruneControl deletes control frames older than beforeMs.
func (s *Spool) PruneControl(beforeMs int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM control WHERE recv_ts<?`, beforeMs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PruneOutbox drops terminal (sent/failed) rows older than beforeMs.
func (s *Spool) PruneOutbox(beforeMs int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM outbox WHERE status!='queued' AND created_ts<?`, beforeMs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ---------- pub/sub ----------

// Subscribe returns a channel of live events. If the consumer falls behind the
// channel is closed, signalling the handler to drop the connection so the
// client reconnects and replays from its last_seq.
func (s *Spool) Subscribe() chan Event {
	ch := make(chan Event, 1024)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *Spool) Unsubscribe(ch chan Event) {
	s.mu.Lock()
	if _, ok := s.subs[ch]; ok {
		delete(s.subs, ch)
		close(ch)
	}
	s.mu.Unlock()
}

func (s *Spool) SubCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

func (s *Spool) publish(e Event) {
	s.mu.Lock()
	for ch := range s.subs {
		select {
		case ch <- e:
		default:
			// Slow consumer: drop it; client must reconnect and replay.
			delete(s.subs, ch)
			close(ch)
		}
	}
	s.mu.Unlock()
}

// ---------- consumers ----------

func (s *Spool) TouchConsumer(name string, lastAck int64) {
	if name == "" {
		return
	}
	s.db.Exec(`INSERT INTO consumers(name,last_ack,updated_ts) VALUES(?,?,?)
	           ON CONFLICT(name) DO UPDATE SET last_ack=excluded.last_ack, updated_ts=excluded.updated_ts`,
		name, lastAck, time.Now().UnixMilli())
}

// ---------- outbox ----------

func (s *Spool) EnqueueOutbox(method, path string, body []byte, contentType string) (int64, error) {
	now := time.Now().UnixMilli()
	res, err := s.db.Exec(
		`INSERT INTO outbox(created_ts,method,path,body,content_type,status,next_retry_ts)
		 VALUES(?,?,?,?,?,'queued',?)`,
		now, method, path, body, contentType, now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// RecordSent inserts an already-completed call for the audit trail.
func (s *Spool) RecordSent(method, path string, body []byte, contentType string, respStatus int, respBody []byte) error {
	now := time.Now().UnixMilli()
	_, err := s.db.Exec(
		`INSERT INTO outbox(created_ts,method,path,body,content_type,status,resp_status,resp_body,attempts)
		 VALUES(?,?,?,?,?,'sent',?,?,1)`,
		now, method, path, body, contentType, respStatus, respBody)
	return err
}

func (s *Spool) MarkOutboxSent(id int64, respStatus int, respBody []byte) error {
	_, err := s.db.Exec(
		`UPDATE outbox SET status='sent', resp_status=?, resp_body=?, attempts=attempts+1, last_error=NULL WHERE id=?`,
		respStatus, respBody, id)
	return err
}

func (s *Spool) MarkOutboxFailed(id int64, lastErr string) error {
	_, err := s.db.Exec(
		`UPDATE outbox SET status='failed', last_error=? WHERE id=?`, lastErr, id)
	return err
}

func (s *Spool) MarkOutboxRetry(id int64, nextRetryMs int64, lastErr string) error {
	_, err := s.db.Exec(
		`UPDATE outbox SET attempts=attempts+1, next_retry_ts=?, last_error=? WHERE id=?`,
		nextRetryMs, lastErr, id)
	return err
}

func (s *Spool) DueOutbox(nowMs int64, limit int) ([]OutboxEntry, error) {
	rows, err := s.db.Query(
		`SELECT id,created_ts,method,path,body,content_type,status,attempts
		 FROM outbox WHERE status='queued' AND next_retry_ts<=? ORDER BY id LIMIT ?`,
		nowMs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.CreatedMs, &e.Method, &e.Path, &e.Body, &e.ContentType, &e.Status, &e.Attempts); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Spool) GetOutbox(id int64) (*OutboxEntry, error) {
	var e OutboxEntry
	var respBody []byte
	var respStatus sql.NullInt64
	var contentType, lastErr sql.NullString
	err := s.db.QueryRow(
		`SELECT id,created_ts,method,path,body,content_type,status,resp_status,resp_body,attempts,next_retry_ts,last_error
		 FROM outbox WHERE id=?`, id).Scan(
		&e.ID, &e.CreatedMs, &e.Method, &e.Path, &e.Body, &contentType, &e.Status,
		&respStatus, &respBody, &e.Attempts, &e.NextRetryMs, &lastErr)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	e.ContentType = contentType.String
	e.LastError = lastErr.String
	e.RespBody = respBody
	if respStatus.Valid {
		v := int(respStatus.Int64)
		e.RespStatus = &v
	}
	return &e, nil
}
