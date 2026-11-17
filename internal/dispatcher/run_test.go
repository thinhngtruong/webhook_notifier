package dispatcher_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"webhook-notifier/internal/dispatcher"
	"webhook-notifier/internal/event"
	"webhook-notifier/internal/store"
)

// flakyStore fails the first FanOut, then records event IDs it stored and
// signals each successful call on stored.
type flakyStore struct {
	mu     sync.Mutex
	calls  int
	events []string
	stored chan struct{}
}

func (s *flakyStore) FanOut(_ context.Context, ds []store.NewDelivery) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return 0, errors.New("transient")
	}
	for _, d := range ds {
		s.events = append(s.events, d.EventID)
	}
	s.stored <- struct{}{}
	return int64(len(ds)), nil
}

func TestRunRetriesAfterErrorAndStopsOnCancel(t *testing.T) {
	// Visibility 0: an unacked event is immediately receivable again.
	q := event.NewMemQueue(0, nil)
	q.Publish(evt("e1", "subscriber.created", "account-1"))
	st := &flakyStore{stored: make(chan struct{}, 10)}
	d := dispatcher.New(q, st, dispatcher.NewWebhookCache(time.Minute, nil, oneHook), 10, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, time.Millisecond) }()

	select {
	case <-st.stored:
	case <-time.After(5 * time.Second):
		t.Fatal("event was never stored after the transient failure")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.calls < 2 || len(st.events) != 1 || st.events[0] != "e1" {
		t.Fatalf("calls = %d, stored = %v", st.calls, st.events)
	}
}
