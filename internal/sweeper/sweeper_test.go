package sweeper

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/BilalWarraich/order-pipeline/internal/order"
	"github.com/BilalWarraich/order-pipeline/internal/worker"
)

func addOrder(t *testing.T, s *order.MemStore, id string, age time.Duration, st order.Status) {
	t.Helper()
	err := s.Create(context.Background(), &order.Order{
		ID: id, CustomerID: "c", Status: st, CreatedAt: time.Now().Add(-age),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSweepOnlyOldPending(t *testing.T) {
	store := order.NewMemStore()
	addOrder(t, store, "old-pending", time.Hour, order.StatusPending)
	addOrder(t, store, "new-pending", time.Second, order.StatusPending)
	addOrder(t, store, "old-done", time.Hour, order.StatusDone)

	var got []string
	s := &Sweeper{
		Store:  store,
		MaxAge: time.Minute,
		Submit: func(id string) error { got = append(got, id); return nil },
	}
	n, err := s.sweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(got) != 1 || got[0] != "old-pending" {
		t.Fatalf("n=%d got=%v", n, got)
	}
}

func TestSweepStopsWhenQueueFull(t *testing.T) {
	store := order.NewMemStore()
	for _, id := range []string{"a", "b", "c"} {
		addOrder(t, store, id, time.Hour, order.StatusPending)
	}

	calls := 0
	s := &Sweeper{
		Store:  store,
		MaxAge: time.Minute,
		Submit: func(id string) error { calls++; return worker.ErrQueueFull },
	}
	n, _ := s.sweepOnce(context.Background())
	if n != 0 || calls != 1 {
		t.Fatalf("should stop at first full queue, n=%d calls=%d", n, calls)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	store := order.NewMemStore()
	addOrder(t, store, "x", time.Hour, order.StatusPending)

	var mu sync.Mutex
	submitted := 0
	s := &Sweeper{
		Store:    store,
		Interval: 5 * time.Millisecond,
		MaxAge:   time.Minute,
		Submit: func(id string) error {
			mu.Lock()
			submitted++
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run didn't return after cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if submitted == 0 {
		t.Fatal("expected at least one sweep")
	}
}
