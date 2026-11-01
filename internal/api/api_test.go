package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/BilalWarraich/order-pipeline/internal/order"
	"github.com/BilalWarraich/order-pipeline/internal/worker"
)

func setup(t *testing.T) (*httptest.Server, order.Store) {
	store := order.NewMemStore()
	// no-op handler, we only care about the http side here
	pool := worker.NewPool(1, 10, func(ctx context.Context, id string) error { return nil })
	t.Cleanup(func() { pool.Shutdown(context.Background()) })

	srv := httptest.NewServer(New(store, pool).Routes())
	t.Cleanup(srv.Close)
	return srv, store
}

func TestCreateAndGet(t *testing.T) {
	srv, _ := setup(t)

	body := `{"customer_id":"c42","items":[{"sku":"KB-01","qty":2,"price_cents":4999}]}`
	resp, err := http.Post(srv.URL+"/orders", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}

	var o order.Order
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
		t.Fatal(err)
	}
	if o.TotalCts != 9998 {
		t.Errorf("total = %d", o.TotalCts)
	}

	resp2, err := http.Get(srv.URL + "/orders/" + o.ID)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("get status %d", resp2.StatusCode)
	}
}

func TestCreateBadInput(t *testing.T) {
	srv, _ := setup(t)

	for _, body := range []string{
		`not json`,
		`{"customer_id":"","items":[]}`,
		`{"customer_id":"c1","items":[{"sku":"x","qty":0}]}`,
	} {
		resp, err := http.Post(srv.URL+"/orders", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%q -> %d, want 400", body, resp.StatusCode)
		}
	}
}

func TestGetMissing(t *testing.T) {
	srv, _ := setup(t)
	resp, _ := http.Get(srv.URL + "/orders/nope")
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestIndex(t *testing.T) {
	srv, _ := setup(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestMetrics(t *testing.T) {
	srv, _ := setup(t)

	// hit a route with an id in it first
	resp, _ := http.Get(srv.URL + "/orders/ord_123")
	resp.Body.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	body := string(b)

	for _, want := range []string{
		`http_request_duration_seconds_count{code="404",method="GET",route="/orders/{id}"} 1`,
		"orders_queue_depth 0",
		"orders_processed_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
	if strings.Contains(body, "ord_123") {
		t.Error("order id leaked into a label, that's a cardinality bomb")
	}
}

// returns status + headers, not the response itself, the body is already closed here
func postWithKey(t *testing.T, url, key, body string) (int, http.Header, order.Order) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/orders", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var o order.Order
	_ = json.NewDecoder(resp.Body).Decode(&o)
	return resp.StatusCode, resp.Header, o
}

func TestIdempotencyKey(t *testing.T) {
	srv, _ := setup(t)
	body := `{"customer_id":"c1","items":[{"sku":"KB-01","qty":1,"price_cents":4999}]}`

	code1, _, o1 := postWithKey(t, srv.URL, "abc-123", body)
	if code1 != http.StatusAccepted {
		t.Fatalf("first: %d", code1)
	}

	// client retries, e.g. after a timeout
	code2, h2, o2 := postWithKey(t, srv.URL, "abc-123", body)
	if code2 != http.StatusOK || h2.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry: status %d, replayed=%q", code2, h2.Get("Idempotent-Replayed"))
	}
	if o2.ID != o1.ID {
		t.Fatalf("retry created a new order: %s vs %s", o2.ID, o1.ID)
	}

	// same key, different order -> refuse
	other := `{"customer_id":"c1","items":[{"sku":"MOUSE","qty":1,"price_cents":999}]}`
	if code3, _, _ := postWithKey(t, srv.URL, "abc-123", other); code3 != http.StatusUnprocessableEntity {
		t.Fatalf("different body with same key: %d", code3)
	}

	// no key -> two separate orders like before
	_, _, a := postWithKey(t, srv.URL, "", body)
	_, _, b := postWithKey(t, srv.URL, "", body)
	if a.ID == b.ID {
		t.Fatal("requests without a key shouldn't be deduped")
	}
}

func TestIdempotencyKeyConcurrent(t *testing.T) {
	srv, store := setup(t)
	body := `{"customer_id":"c1","items":[{"sku":"KB-01","qty":1,"price_cents":4999}]}`

	// a double-clicked "buy" button: same request fired at the same time
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			postWithKey(t, srv.URL, "double-click", body)
		}()
	}
	wg.Wait()

	all, _ := store.List(context.Background(), 100)
	if len(all) != 1 {
		t.Fatalf("expected 1 order, got %d", len(all))
	}
}
