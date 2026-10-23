package order

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemStore is used when no DATABASE_URL is set, and in tests.
type MemStore struct {
	mu     sync.RWMutex
	orders map[string]*Order
}

func NewMemStore() *MemStore {
	return &MemStore{orders: make(map[string]*Order)}
}

func (m *MemStore) Create(_ context.Context, o *Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *o
	m.orders[o.ID] = &cp
	return nil
}

func (m *MemStore) Get(_ context.Context, id string) (*Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	o, ok := m.orders[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *o
	return &cp, nil
}

func (m *MemStore) List(_ context.Context, limit int) ([]*Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Order, 0, len(m.orders))
	for _, o := range m.orders {
		cp := *o
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) UpdateStatus(_ context.Context, id string, s Status, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return ErrNotFound
	}
	o.Status = s
	o.Error = errMsg
	o.UpdatedAt = time.Now().UTC()
	return nil
}

func (m *MemStore) Claim(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return false, ErrNotFound
	}
	if o.Status != StatusPending {
		return false, nil
	}
	o.Status = StatusProcessing
	o.UpdatedAt = time.Now().UTC()
	return true, nil
}
