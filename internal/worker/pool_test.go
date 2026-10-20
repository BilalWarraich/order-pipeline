package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolProcessesAll(t *testing.T) {
	var count atomic.Int32
	p := NewPool(4, 100, func(ctx context.Context, id string) error {
		count.Add(1)
		return nil
	})

	for i := 0; i < 50; i++ {
		if err := p.Submit("x"); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 50 {
		t.Fatalf("expected 50 got %d", count.Load())
	}
}

func TestPoolQueueFull(t *testing.T) {
	block := make(chan struct{})
	p := NewPool(1, 1, func(ctx context.Context, id string) error {
		<-block
		return nil
	})
	defer close(block)

	// first one gets picked up by the worker, second sits in the buffer.
	// might need a tiny sleep so the worker actually grabs the first
	p.Submit("a")
	time.Sleep(10 * time.Millisecond)
	p.Submit("b")

	if err := p.Submit("c"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestSubmitAfterShutdown(t *testing.T) {
	p := NewPool(1, 1, func(ctx context.Context, id string) error { return nil })
	p.Shutdown(context.Background())
	if err := p.Submit("a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestStatsCountsFailures(t *testing.T) {
	p := NewPool(2, 10, func(ctx context.Context, id string) error {
		if id == "bad" {
			return errors.New("nope")
		}
		return nil
	})
	p.Submit("ok")
	p.Submit("bad")
	p.Submit("ok")
	p.Shutdown(context.Background())

	s := p.Stats()
	if s.Processed != 2 || s.Failed != 1 {
		t.Fatalf("unexpected stats %+v", s)
	}
}
