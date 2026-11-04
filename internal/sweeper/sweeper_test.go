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

func TestRecoversStuckProcessing(t *testing.T) {
	store := order.NewMemStore()
	ctx := context.Background()
	addOrder(t, store, "crashed", time.Hour, order.StatusPending)
	addOrder(t, store, "working", time.Hour, order.StatusPending)
	store.Claim(ctx, "crashed")
	store.Claim(ctx, "working")

	// "crashed" has been processing for 10 min, "working" just started
	store.SetUpdatedAtForTest("crashed", time.Now().Add(-10*time.Minute))

	var got []string
	s := &Sweeper{
		Store:           store,
		MaxAge:          time.Minute,
		StuckProcessing: 5 * time.Minute,
		Submit:          func(id string) error { got = append(got, id); return nil },
	}
	if _, err := s.sweepOnce(ctx); err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || got[0] != "crashed" {
		t.Fatalf("expected only the crashed order to be requeued, got %v", got)
	}
	if o, _ := store.Get(ctx, "working"); o.Status != order.StatusProcessing {
		t.Fatalf("order a worker is still on shouldn't be touched, status=%s", o.Status)
	}
	// and the requeued one can be claimed again by a worker
	if ok, _ := store.Claim(ctx, "crashed"); !ok {
		t.Fatal("reset order should be claimable")
	}
}

func TestStuckProcessingOff(t *testing.T) {
	store := order.NewMemStore()
	ctx := context.Background()
	addOrder(t, store, "x", time.Hour, order.StatusPending)
	store.Claim(ctx, "x")
	store.SetUpdatedAtForTest("x", time.Now().Add(-time.Hour))

	s := &Sweeper{Store: store, MaxAge: time.Minute, Submit: func(string) error { return nil }}
	s.sweepOnce(ctx)
	if o, _ := store.Get(ctx, "x"); o.Status != order.StatusProcessing {
		t.Fatal("StuckProcessing=0 should leave processing orders alone")
	}
}
