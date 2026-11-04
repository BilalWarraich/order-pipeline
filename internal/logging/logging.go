// Package logging sets up slog from env vars.
//
//	LOG_FORMAT=json   one json object per line (what render's log viewer / any log tool wants)
//	LOG_FORMAT=text   key=value, nicer locally (default)
//	LOG_LEVEL=debug|info|warn|error   default info
package logging

import (
	"io"
	"log/slog"
	"strings"
)

func New(w io.Writer, format, level string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	if strings.EqualFold(format, "json") {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}
