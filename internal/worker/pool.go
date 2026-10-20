package worker

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
)

var (
	ErrQueueFull = errors.New("queue is full")
	ErrClosed    = errors.New("pool is closed")
)

type Handler func(ctx context.Context, id string) error

// Pool is a fixed number of goroutines reading order ids off a buffered channel.
// Submit never blocks - if the buffer is full the caller gets ErrQueueFull
// and can return a 503 or whatever.
type Pool struct {
	jobs    chan string
	handler Handler
	wg      sync.WaitGroup

	mu     sync.RWMutex
	closed bool

	processed atomic.Int64
	failed    atomic.Int64
}

func NewPool(workers, queueSize int, h Handler) *Pool {
	p := &Pool{
		jobs:    make(chan string, queueSize),
		handler: h,
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.run(i)
	}
	return p
}

func (p *Pool) run(n int) {
	defer p.wg.Done()
	for id := range p.jobs {
		// each job gets its own ctx. we dont cancel in-flight jobs on shutdown,
		// we let them finish (Shutdown has its own deadline)
		if err := p.handler(context.Background(), id); err != nil {
			p.failed.Add(1)
			log.Printf("worker %d: order %s failed: %v", n, id, err)
			continue
		}
		p.processed.Add(1)
	}
}

func (p *Pool) Submit(id string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.jobs <- id:
		return nil
	default:
		return ErrQueueFull
	}
}

// Shutdown stops accepting work and waits for the queue to drain,
// or until ctx is done.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Stats struct {
	Queued    int   `json:"queued"`
	Processed int64 `json:"processed"`
	Failed    int64 `json:"failed"`
}

func (p *Pool) Stats() Stats {
	return Stats{
		Queued:    len(p.jobs),
		Processed: p.processed.Load(),
		Failed:    p.failed.Load(),
	}
}
