package order

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestOrder(t *testing.T, s Store, total int64) string {
	t.Helper()
	o := &Order{
		ID:         "ord_test",
		CustomerID: "c1",
		Items:      []Item{{SKU: "abc", Qty: 1, PriceCts: total}},
		Status:     StatusPending,
		CreatedAt:  time.Now(),
	}
	o.TotalCts = o.Total()
	if err := s.Create(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	return o.ID
}

func TestProcessHappyPath(t *testing.T) {
	s := NewMemStore()
	id := newTestOrder(t, s, 1000)

	p := &Processor{Store: s}
	if err := p.Process(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	o, _ := s.Get(context.Background(), id)
	if o.Status != StatusDone {
		t.Fatalf("status = %s", o.Status)
	}
}

func TestProcessFlagsBigOrders(t *testing.T) {
	s := NewMemStore()
	id := newTestOrder(t, s, 1_000_000)

	p := &Processor{Store: s}
	if err := p.Process(context.Background(), id); err == nil {
		t.Fatal("expected error")
	}
	o, _ := s.Get(context.Background(), id)
	if o.Status != StatusFailed || o.Error == "" {
		t.Fatalf("got %+v", o)
	}
}

func TestRetry(t *testing.T) {
	calls := 0
	err := retry(context.Background(), 3, func() error {
		calls++
		if calls < 3 {
			return errors.New("flaky")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}

	calls = 0
	err = retry(context.Background(), 5, func() error {
		calls++
		return errPermanent{errors.New("no")}
	})
	if err == nil || calls != 1 {
		t.Fatalf("permanent error should stop retries, calls=%d", calls)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		o    Order
		ok   bool
	}{
		{"ok", Order{CustomerID: "c", Items: []Item{{SKU: "a", Qty: 1, PriceCts: 10}}}, true},
		{"no customer", Order{Items: []Item{{SKU: "a", Qty: 1}}}, false},
		{"no items", Order{CustomerID: "c"}, false},
		{"zero qty", Order{CustomerID: "c", Items: []Item{{SKU: "a", Qty: 0}}}, false},
		{"neg price", Order{CustomerID: "c", Items: []Item{{SKU: "a", Qty: 1, PriceCts: -5}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.o.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

// countingStore counts how many times an order actually gets processed
type countingStore struct {
	*MemStore
	done atomic.Int32
}

func (c *countingStore) UpdateStatus(ctx context.Context, id string, s Status, msg string) error {
	if s == StatusDone {
		c.done.Add(1)
	}
	return c.MemStore.UpdateStatus(ctx, id, s, msg)
}

func TestSameOrderProcessedOnce(t *testing.T) {
	s := &countingStore{MemStore: NewMemStore()}
	id := newTestOrder(t, s, 1000)
	p := &Processor{Store: s}

	// simulate the order being queued twice (e.g. by the sweeper) and picked up
	// by several workers at the same time
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Process(context.Background(), id); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if n := s.done.Load(); n != 1 {
		t.Fatalf("order was processed %d times", n)
	}
}

func TestClaim(t *testing.T) {
	s := NewMemStore()
	id := newTestOrder(t, s, 100)
	ctx := context.Background()

	if ok, _ := s.Claim(ctx, id); !ok {
		t.Fatal("first claim should win")
	}
	if ok, _ := s.Claim(ctx, id); ok {
		t.Fatal("second claim should lose")
	}
	if _, err := s.Claim(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}
