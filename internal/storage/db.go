package storage

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"

	assets "github.com/gofurry/metacritic-harvester"
)

func Open(ctx context.Context, path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite database: %w", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set sqlite journal_mode WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set sqlite busy_timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable sqlite foreign_keys: %w", err)
	}

	if err := InitSchema(ctx, db, assets.SchemaSQL); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func OpenReadOnly(ctx context.Context, path string) (*sql.DB, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("database file does not exist: %s", path)
		}
		return nil, fmt.Errorf("stat database file: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("database path is a directory: %s", path)
	}

	uri, err := sqliteReadOnlyURI(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database read-only: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite database read-only: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set sqlite busy_timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable sqlite foreign_keys: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA query_only=ON;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable sqlite query_only: %w", err)
	}

	return db, nil
}

func Checkpoint(ctx context.Context, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("database file does not exist: %s", path)
		}
		return fmt.Errorf("stat database file: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("database path is a directory: %s", path)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open sqlite database for checkpoint: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite database for checkpoint: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000;"); err != nil {
		return fmt.Errorf("set sqlite busy_timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);"); err != nil {
		return fmt.Errorf("checkpoint sqlite wal: %w", err)
	}

	return nil
}

func sqliteReadOnlyURI(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve database path: %w", err)
	}
	uriPath := filepath.ToSlash(abs)
	if len(uriPath) >= 2 && uriPath[1] == ':' && !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	u := url.URL{
		Scheme: "file",
		Path:   uriPath,
	}
	query := url.Values{}
	query.Set("mode", "ro")
	u.RawQuery = query.Encode()
	return u.String(), nil
}
