package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, "json", "info").Info("hello", "order_id", "ord_1")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("not json: %q", buf.String())
	}
	if m["msg"] != "hello" || m["order_id"] != "ord_1" {
		t.Fatalf("got %v", m)
	}
}

func TestLevel(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "text", "warn")
	l.Info("quiet")
	l.Warn("loud")
	if strings.Contains(buf.String(), "quiet") || !strings.Contains(buf.String(), "loud") {
		t.Fatalf("level filter didn't work: %q", buf.String())
	}
}
