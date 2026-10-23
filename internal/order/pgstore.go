package order

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGStore struct {
	db *pgxpool.Pool
}

func NewPGStore(db *pgxpool.Pool) *PGStore {
	return &PGStore{db: db}
}

func (s *PGStore) Create(ctx context.Context, o *Order) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `
		INSERT INTO orders (id, customer_id, items, total_cents, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		o.ID, o.CustomerID, items, o.TotalCts, o.Status, o.CreatedAt, o.UpdatedAt)
	return err
}

func (s *PGStore) Get(ctx context.Context, id string) (*Order, error) {
	row := s.db.QueryRow(ctx, `
		SELECT id, customer_id, items, total_cents, status, coalesce(error, ''), created_at, updated_at
		FROM orders WHERE id = $1`, id)
	o, err := scanOrder(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return o, err
}

func (s *PGStore) List(ctx context.Context, limit int) ([]*Order, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, customer_id, items, total_cents, status, coalesce(error, ''), created_at, updated_at
		FROM orders ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *PGStore) UpdateStatus(ctx context.Context, id string, st Status, errMsg string) error {
	tag, err := s.db.Exec(ctx,
		`UPDATE orders SET status = $2, error = nullif($3, ''), updated_at = now() WHERE id = $1`,
		id, st, errMsg)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func scanOrder(row pgx.Row) (*Order, error) {
	var o Order
	var items []byte
	err := row.Scan(&o.ID, &o.CustomerID, &items, &o.TotalCts, &o.Status, &o.Error, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(items, &o.Items); err != nil {
		return nil, err
	}
	return &o, nil
}

// Claim relies on the WHERE status = 'pending' so two workers racing on the same
// order can't both win, postgres only lets one UPDATE match.
func (s *PGStore) Claim(ctx context.Context, id string) (bool, error) {
	tag, err := s.db.Exec(ctx,
		`UPDATE orders SET status = 'processing', updated_at = now() WHERE id = $1 AND status = 'pending'`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) StalePending(ctx context.Context, before time.Time, limit int) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id FROM orders
		WHERE status = 'pending' AND created_at < $1
		ORDER BY created_at
		LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
