package dispatcher

import (
	"context"
	"sync"
	"time"

	"webhook-notifier/internal/store"
)

// LoadFunc loads an account's active webhooks.
type LoadFunc func(ctx context.Context, accountID string) ([]store.Webhook, error)

// WebhookCache caches each account's active webhooks for a TTL, so the
// dispatcher does not query the webhooks table for every event. A webhook
// added or disabled through the API is picked up within one TTL.
type WebhookCache struct {
	ttl  time.Duration
	now  func() time.Time
	load LoadFunc

	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	hooks   []store.Webhook
	expires time.Time
}

// NewWebhookCache returns a cache. A nil now uses time.Now.
func NewWebhookCache(ttl time.Duration, now func() time.Time, load LoadFunc) *WebhookCache {
	if now == nil {
		now = time.Now
	}
	return &WebhookCache{ttl: ttl, now: now, load: load, entries: map[string]cacheEntry{}}
}

// Get returns the account's active webhooks, loading them on a miss or
// after the TTL. Load errors are returned and not cached.
func (c *WebhookCache) Get(ctx context.Context, accountID string) ([]store.Webhook, error) {
	c.mu.Lock()
	e, ok := c.entries[accountID]
	c.mu.Unlock()
	if ok && c.now().Before(e.expires) {
		return e.hooks, nil
	}

	hooks, err := c.load(ctx, accountID)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.entries[accountID] = cacheEntry{hooks: hooks, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return hooks, nil
}
