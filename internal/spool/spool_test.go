package spool_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/elwina/qqbot-adapter/internal/spool"
)

// 回归：v1 最后一行是控制帧时，导入后新事件不得复用该 seq（消费端位点冲突）。
func TestPinEventSeqFloor(t *testing.T) {
	sp, err := spool.Open(t.TempDir() + "/a.db")
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	s := int64(1)
	// 模拟导入：业务行进 events（显式 id），控制行进 control（不进 events）
	if err := sp.InsertEventAt(4, time.Now().UnixMilli(), 0, "GROUP_MESSAGE_CREATE", &s,
		json.RawMessage(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := sp.InsertControl(time.Now().UnixMilli(), "RESUMED", &s, json.RawMessage(`{}`), int64Ptr(5)); err != nil {
		t.Fatal(err)
	}
	if err := sp.PinEventSeqFloor(5); err != nil {
		t.Fatal(err)
	}
	id, err := sp.InsertEvent(time.Now().UnixMilli(), 0, "GROUP_MESSAGE_CREATE", &s,
		json.RawMessage(`{"x":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if id <= 5 {
		t.Fatalf("新事件 id=%d，复用了 ≤5 的号", id)
	}
}

func int64Ptr(v int64) *int64 { return &v }
