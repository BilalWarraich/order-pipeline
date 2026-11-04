// Package sweeper re-queues orders that got stuck, either in pending (queue was
// full, or the process died before a worker got to them) or in processing (the
// process died while a worker was on it).
package sweeper

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/BilalWarraich/order-pipeline/internal/worker"
)

type Finder interface {
	StalePending(ctx context.Context, before time.Time, limit int) ([]string, error)
	ResetStuckProcessing(ctx context.Context, before time.Time) (int, error)
}

type Sweeper struct {
	Store    Finder
	Submit   func(id string) error
	Interval time.Duration // how often to look
	MaxAge   time.Duration // how old a pending order has to be before we touch it
	// how long an order can sit in processing before we assume the worker died.
	// has to be way longer than the processor timeout, otherwise a slow-but-alive
	// worker gets its order taken away. 0 = don't do this.
	StuckProcessing time.Duration
	Batch           int
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
				slog.Error("sweeper", "err", err)
			} else if n > 0 {
				slog.Info("sweeper requeued stuck orders", "count", n)
			}
		}
	}
}

func (s *Sweeper) sweepOnce(ctx context.Context) (int, error) {
	batch := s.Batch
	if batch <= 0 {
		batch = 100
	}
	if s.StuckProcessing > 0 {
		reset, err := s.Store.ResetStuckProcessing(ctx, time.Now().Add(-s.StuckProcessing))
		if err != nil {
			return 0, err
		}
		if reset > 0 {
			// they're pending again with an old created_at, so the query below picks them up
			slog.Warn("orders were stuck in processing, reset to pending", "count", reset)
		}
	}

	ids, err := s.Store.StalePending(ctx, time.Now().Add(-s.MaxAge), batch)
	if err != nil {
		return 0, err
	}

	n := 0
	for _, id := range ids {
		slog.Debug("sweeper requeueing", "order_id", id)
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
