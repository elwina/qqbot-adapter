// import-jsonl migrates v1 events.jsonl into the v2 SQLite spool.
//
// Line numbers become events.id so consumers' last_seq replays seamlessly.
// READY/RESUMED lines go to the control table (orig_seq = line number),
// leaving gaps in events.id which replay tolerates (id > last_seq).
//
// Usage: import-jsonl -config config.json -file events.jsonl
// Idempotent: rows insert with INSERT OR IGNORE on the explicit id.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"qqbot-adapter/internal/config"
	"qqbot-adapter/internal/spool"
)

var controlTypes = map[string]bool{"READY": true, "RESUMED": true}

func main() {
	cfgPath := flag.String("config", "config.json", "config file path")
	file := flag.String("file", "", "events.jsonl path")
	batch := flag.Int("batch", 2000, "rows per transaction")
	flag.Parse()
	if *file == "" {
		log.Fatal("需要 -file events.jsonl")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	baseDir, _ := filepath.Abs(filepath.Dir(*cfgPath))
	dbPath := config.Resolve(baseDir, cfg.DBPath)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		log.Fatalf("建 state 目录失败: %v", err)
	}
	sp, err := spool.Open(dbPath)
	if err != nil {
		log.Fatalf("打开 spool 失败: %v", err)
	}
	defer sp.Close()

	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("打开 %s: %v", *file, err)
	}
	defer f.Close()

	var (
		lineNo      int64
		imported    int
		controlRows int
		skipped     int
		pending     int
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	tx, err := sp.DB().Begin()
	if err != nil {
		log.Fatalf("begin tx: %v", err)
	}
	commit := func() {
		if err := tx.Commit(); err != nil {
			log.Fatalf("commit: %v", err)
		}
		tx, err = sp.DB().Begin()
		if err != nil {
			log.Fatalf("begin tx: %v", err)
		}
		pending = 0
	}
	for sc.Scan() {
		lineNo++
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var row struct {
			RecvTS float64         `json:"recv_ts"`
			Op     int             `json:"op"`
			T      string          `json:"t"`
			S      *int64          `json:"s"`
			D      json.RawMessage `json:"d"`
		}
		if err := json.Unmarshal(raw, &row); err != nil || row.T == "" {
			skipped++
			continue
		}
		recvMs := int64(row.RecvTS * 1000)
		var cerr error
		if controlTypes[row.T] {
			_, cerr = tx.Exec(
				`INSERT INTO control(recv_ts,t,s,payload,orig_seq) VALUES(?,?,?,?,?)`,
				recvMs, row.T, row.S, string(row.D), lineNo)
			if cerr == nil {
				controlRows++
			}
		} else {
			_, cerr = tx.Exec(
				`INSERT OR IGNORE INTO events(id,recv_ts,op,t,s,payload) VALUES(?,?,?,?,?,?)`,
				lineNo, recvMs, row.Op, row.T, row.S, string(row.D))
			if cerr == nil {
				imported++
			}
		}
		if cerr != nil {
			tx.Rollback()
			log.Fatalf("第 %d 行写入失败: %v", lineNo, cerr)
		}
		pending++
		if pending >= *batch {
			commit()
		}
	}
	if err := sc.Err(); err != nil {
		tx.Rollback()
		log.Fatalf("读取文件失败: %v", err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}
	last, _ := sp.LastSeq()
	fmt.Printf("导入完成：lines=%d events=%d control=%d skipped=%d max_seq=%d\n",
		lineNo, imported, controlRows, skipped, last)
}
