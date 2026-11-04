package order

import "time"

// SetUpdatedAtForTest lets tests in other packages fake an order that's been
// sitting around for a while. Not for real code.
func (m *MemStore) SetUpdatedAtForTest(id string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o, ok := m.orders[id]; ok {
		o.UpdatedAt = t
	}
}
