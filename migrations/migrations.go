// Package migrations runs the .sql files in this dir on startup.
// They're all written to be idempotent (IF NOT EXISTS) so running them on
// every boot is fine. Not a real migration tool, but good enough for now.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

func Run(ctx context.Context, db *pgxpool.Pool) error {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)

	for _, name := range names {
		sql, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := db.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}
