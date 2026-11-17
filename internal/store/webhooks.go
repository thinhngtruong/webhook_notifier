package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Webhook is a partner subscription on an account.
type Webhook struct {
	ID        UUID
	AccountID string
	PostURL   string
	Events    []string
	Secret    string
	Status    string // "active" or "disabled"
}

// Subscribes reports whether the webhook wants events named eventName.
func (w Webhook) Subscribes(eventName string) bool {
	for _, e := range w.Events {
		if e == eventName {
			return true
		}
	}
	return false
}

// InsertWebhook stores w and returns its ID. A zero w.ID gets a random one.
// The real system owns this table through its webhook API; the PoC uses
// this to seed data.
func (s *Store) InsertWebhook(ctx context.Context, w Webhook) (UUID, error) {
	if w.ID == (UUID{}) {
		w.ID = NewRandomUUID()
	}
	if w.Status == "" {
		w.Status = "active"
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO webhooks (id, account_id, post_url, events, secret, status)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		w.ID, w.AccountID, w.PostURL, w.Events, w.Secret, w.Status)
	if err != nil {
		return UUID{}, fmt.Errorf("insert webhook: %w", err)
	}
	return w.ID, nil
}

// ActiveWebhooks returns the account's active webhooks.
func (s *Store) ActiveWebhooks(ctx context.Context, accountID string) ([]Webhook, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, account_id, post_url, events, secret, status
		   FROM webhooks
		  WHERE account_id = $1 AND status = 'active'
		  ORDER BY id`, accountID)
	if err != nil {
		return nil, fmt.Errorf("active webhooks for %s: %w", accountID, err)
	}
	hooks, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Webhook, error) {
		var w Webhook
		err := r.Scan(&w.ID, &w.AccountID, &w.PostURL, &w.Events, &w.Secret, &w.Status)
		return w, err
	})
	if err != nil {
		return nil, fmt.Errorf("active webhooks for %s: %w", accountID, err)
	}
	return hooks, nil
}

// Webhook returns one webhook by ID, whatever its status.
func (s *Store) Webhook(ctx context.Context, id UUID) (Webhook, error) {
	var w Webhook
	err := s.pool.QueryRow(ctx,
		`SELECT id, account_id, post_url, events, secret, status FROM webhooks WHERE id = $1`, id,
	).Scan(&w.ID, &w.AccountID, &w.PostURL, &w.Events, &w.Secret, &w.Status)
	if err != nil {
		return Webhook{}, fmt.Errorf("webhook %s: %w", id, err)
	}
	return w, nil
}

// DisableWebhook marks a webhook disabled (the partner answered 410 Gone).
func (s *Store) DisableWebhook(ctx context.Context, id UUID) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE webhooks SET status = 'disabled', updated_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("disable webhook %s: %w", id, err)
	}
	return nil
}
