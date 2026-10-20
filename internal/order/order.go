package order

import (
	"context"
	"errors"
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
)

var ErrNotFound = errors.New("order not found")

type Item struct {
	SKU      string `json:"sku"`
	Qty      int    `json:"qty"`
	PriceCts int64  `json:"price_cents"`
}

type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	Items      []Item    `json:"items"`
	TotalCts   int64     `json:"total_cents"`
	Status     Status    `json:"status"`
	Error      string    `json:"error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (o *Order) Total() int64 {
	var t int64
	for _, it := range o.Items {
		t += int64(it.Qty) * it.PriceCts
	}
	return t
}

// Validate does basic sanity checks. not trying to be exhaustive here
func (o *Order) Validate() error {
	if o.CustomerID == "" {
		return errors.New("customer_id is required")
	}
	if len(o.Items) == 0 {
		return errors.New("order needs at least one item")
	}
	for _, it := range o.Items {
		if it.Qty <= 0 {
			return errors.New("qty must be > 0")
		}
		if it.PriceCts < 0 {
			return errors.New("price can't be negative")
		}
	}
	return nil
}

type Store interface {
	Create(ctx context.Context, o *Order) error
	Get(ctx context.Context, id string) (*Order, error)
	List(ctx context.Context, limit int) ([]*Order, error)
	UpdateStatus(ctx context.Context, id string, s Status, errMsg string) error
}
