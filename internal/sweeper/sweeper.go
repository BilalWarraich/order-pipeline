// Package sweeper re-queues orders that got stuck in pending, e.g. because the
// queue was full when they came in or the process died before a worker got to them.
package sweeper

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/BilalWarraich/order-pipeline/internal/worker"
)

type Finder interface {
	StalePending(ctx context.Context, before time.Time, limit int) ([]string, error)
}

type Sweeper struct {
	Store    Finder
	Submit   func(id string) error
	Interval time.Duration // how often to look
	MaxAge   time.Duration // how old a pending order has to be before we touch it
	Batch    int
}

// Run blocks until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := s.sweepOnce(ctx)
			if err != nil {
				log.Printf("sweeper: %v", err)
			} else if n > 0 {
				log.Printf("sweeper: requeued %d stuck orders", n)
			}
		}
	}
}

func (s *Sweeper) sweepOnce(ctx context.Context) (int, error) {
	batch := s.Batch
	if batch <= 0 {
		batch = 100
	}
	ids, err := s.Store.StalePending(ctx, time.Now().Add(-s.MaxAge), batch)
	if err != nil {
		return 0, err
	}

	n := 0
	for _, id := range ids {
		err := s.Submit(id)
		if errors.Is(err, worker.ErrQueueFull) || errors.Is(err, worker.ErrClosed) {
			// try again next tick, no point hammering a full queue
			break
		}
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
