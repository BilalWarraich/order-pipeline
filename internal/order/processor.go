package order

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// Processor fakes the "real" work an order goes through. Inventory and fraud
// checks are independent so they run at the same time, payment only happens
// if both pass.
//
// TODO: swap the fakes for actual http clients at some point
type Processor struct {
	Store   Store
	Timeout time.Duration

	// FailRate is the chance (0-1) any single step fails. handy for testing retries
	FailRate float64
}

func (p *Processor) Process(ctx context.Context, id string) error {
	// don't write to p.Timeout here, Process runs on several workers at once
	timeout := p.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ok, err := p.Store.Claim(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		// already picked up by another worker (or done), nothing to do
		return nil
	}

	o, err := p.Store.Get(ctx, id)
	if err != nil {
		return err
	}

	if err := p.run(ctx, o); err != nil {
		// use a fresh ctx here, the original one might be the thing that timed out
		uctx, ucancel := context.WithTimeout(context.Background(), time.Second)
		defer ucancel()
		_ = p.Store.UpdateStatus(uctx, id, StatusFailed, err.Error())
		return err
	}
	return p.Store.UpdateStatus(ctx, id, StatusDone, "")
}

func (p *Processor) run(ctx context.Context, o *Order) error {
	type result struct {
		step string
		err  error
	}

	steps := map[string]func(context.Context, *Order) error{
		"inventory": p.reserveInventory,
		"fraud":     p.fraudCheck,
	}

	results := make(chan result, len(steps))
	for name, fn := range steps {
		go func(name string, fn func(context.Context, *Order) error) {
			results <- result{name, retry(ctx, 3, func() error { return fn(ctx, o) })}
		}(name, fn)
	}

	var errs []error
	for range steps {
		r := <-results
		if r.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.step, r.err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return retry(ctx, 3, func() error { return p.chargePayment(ctx, o) })
}

func (p *Processor) reserveInventory(ctx context.Context, o *Order) error {
	return p.fakeCall(ctx, 50*time.Millisecond, 200*time.Millisecond)
}

func (p *Processor) fraudCheck(ctx context.Context, o *Order) error {
	// pretend anything over $5k needs a human to look at it
	if o.TotalCts > 500_000 {
		return errPermanent{errors.New("order total too high, flagged for review")}
	}
	return p.fakeCall(ctx, 20*time.Millisecond, 100*time.Millisecond)
}

func (p *Processor) chargePayment(ctx context.Context, o *Order) error {
	return p.fakeCall(ctx, 100*time.Millisecond, 300*time.Millisecond)
}

func (p *Processor) fakeCall(ctx context.Context, min, max time.Duration) error {
	d := min + time.Duration(rand.Int63n(int64(max-min)))
	select {
	case <-time.After(d):
	case <-ctx.Done():
		return ctx.Err()
	}
	if rand.Float64() < p.FailRate {
		return errors.New("upstream error")
	}
	return nil
}

// errPermanent means don't bother retrying
type errPermanent struct{ error }

func retry(ctx context.Context, attempts int, fn func() error) error {
	var err error
	backoff := 50 * time.Millisecond
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		var perm errPermanent
		if errors.As(err, &perm) {
			return perm.error
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}
