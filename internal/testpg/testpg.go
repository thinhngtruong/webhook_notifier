// Package testpg gives integration tests an isolated, migrated database.
// Tests are skipped unless TEST_DATABASE_URL points at a Postgres server
// whose user may create databases (make test sets it).
package testpg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-notifier/migrations"
)

// New creates a throwaway database, applies the migrations and returns a
// pool connected to it. The database is dropped when the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := NewEmpty(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatalf("testpg: migrate: %v", err)
	}
	return pool
}

// NewEmpty is New without migrations.
func NewEmpty(t testing.TB) *pgxpool.Pool {
	t.Helper()
	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("testpg: connect admin: %v", err)
	}
	defer admin.Close(ctx)

	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "test_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("testpg: create database: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		t.Fatalf("testpg: parse url: %v", err)
	}
	cfg.ConnConfig.Database = name
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("testpg: connect: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminURL)
		if err != nil {
			t.Logf("testpg: cleanup connect: %v", err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Logf("testpg: drop %s: %v", name, err)
		}
	})
	return pool
}
