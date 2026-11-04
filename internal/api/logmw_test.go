package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/BilalWarraich/order-pipeline/internal/order"
	"github.com/BilalWarraich/order-pipeline/internal/worker"
)

// safeBuf because the server logs from its own goroutines while the test reads
type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) lines(t *testing.T) []map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(s.b.Bytes()))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("log line isn't json: %q", sc.Text())
		}
		out = append(out, m)
	}
	return out
}

func setupWithLogs(t *testing.T) (*httptest.Server, *safeBuf) {
	buf := &safeBuf{}
	pool := worker.NewPool(1, 10, func(ctx context.Context, id string) error { return nil })
	t.Cleanup(func() { pool.Shutdown(context.Background()) })

	s := New(order.NewMemStore(), pool)
	s.Log = slog.New(slog.NewJSONHandler(buf, nil))
	srv := httptest.NewServer(s.Routes())
	t.Cleanup(srv.Close)
	return srv, buf
}

func TestRequestLog(t *testing.T) {
	srv, buf := setupWithLogs(t)

	resp, _ := http.Get(srv.URL + "/orders/ord_nope")
	resp.Body.Close()

	var reqLine map[string]any
	for _, l := range buf.lines(t) {
		if l["msg"] == "request" {
			reqLine = l
		}
	}
	if reqLine == nil {
		t.Fatal("no request log line")
	}
	if reqLine["route"] != "/orders/{id}" || reqLine["path"] != "/orders/ord_nope" {
		t.Errorf("route/path wrong: %v", reqLine)
	}
	if reqLine["status"] != float64(404) || reqLine["method"] != "GET" {
		t.Errorf("status/method wrong: %v", reqLine)
	}
	if id, _ := reqLine["request_id"].(string); id == "" {
		t.Errorf("missing request_id: %v", reqLine)
	}
	if _, ok := reqLine["duration_ms"].(float64); !ok {
		t.Errorf("missing duration_ms: %v", reqLine)
	}
}

// the "order created" line has to carry both ids, that's what lets you go from
// a request to everything the workers logged about its order
func TestOrderCreatedLogLinksRequestToOrder(t *testing.T) {
	srv, buf := setupWithLogs(t)

	body := `{"customer_id":"c1","items":[{"sku":"A","qty":1,"price_cents":100}]}`
	resp, err := http.Post(srv.URL+"/orders", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var o order.Order
	_ = json.NewDecoder(resp.Body).Decode(&o)
	resp.Body.Close()

	var created, request map[string]any
	for _, l := range buf.lines(t) {
		switch l["msg"] {
		case "order created":
			created = l
		case "request":
			request = l
		}
	}
	if created == nil || created["order_id"] != o.ID {
		t.Fatalf("order created line missing or wrong order_id: %v", created)
	}
	if created["request_id"] == "" || created["request_id"] != request["request_id"] {
		t.Fatalf("request_id doesn't match between lines: %v vs %v", created["request_id"], request["request_id"])
	}
}
