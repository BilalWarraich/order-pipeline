package order

import (
	"context"
	"errors"
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
