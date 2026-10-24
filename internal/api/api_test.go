package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
