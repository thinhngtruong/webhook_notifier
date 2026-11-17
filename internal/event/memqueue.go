package event

import (
	"context"
	"sync"
	"time"
)

// MemQueue is an in-memory Source with at-least-once semantics: a received
// event stays invisible for the visibility timeout and is delivered again if
// it has not been acked by then.
type MemQueue struct {
	visibility time.Duration
	now        func() time.Time

	mu       sync.Mutex
	ready    []Event
	inFlight map[string]inFlight
	dead     map[string]string // id -> reason
}

type inFlight struct {
	event    Event
	deadline time.Time
}

// NewMemQueue returns an empty queue. A nil now uses time.Now.
func NewMemQueue(visibility time.Duration, now func() time.Time) *MemQueue {
	if now == nil {
		now = time.Now
	}
	return &MemQueue{visibility: visibility, now: now, inFlight: map[string]inFlight{}, dead: map[string]string{}}
}

// Publish adds events to the queue.
func (q *MemQueue) Publish(events ...Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ready = append(q.ready, events...)
}

// Receive implements Source.
func (q *MemQueue) Receive(ctx context.Context, max int) ([]Event, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()
	for id, f := range q.inFlight {
		if !now.Before(f.deadline) {
			q.ready = append(q.ready, f.event)
			delete(q.inFlight, id)
		}
	}

	n := min(max, len(q.ready))
	out := append([]Event(nil), q.ready[:n]...)
	q.ready = q.ready[n:]
	for _, e := range out {
		q.inFlight[e.ID] = inFlight{event: e, deadline: now.Add(q.visibility)}
	}
	return out, nil
}

// Ack implements Source.
func (q *MemQueue) Ack(ctx context.Context, ids []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, id := range ids {
		delete(q.inFlight, id)
	}
	return nil
}

// DeadLetter implements Source.
func (q *MemQueue) DeadLetter(ctx context.Context, id, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inFlight, id)
	q.dead[id] = reason
	return nil
}

// DeadLetters returns dead-lettered event IDs and their reasons.
func (q *MemQueue) DeadLetters() map[string]string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]string, len(q.dead))
	for id, r := range q.dead {
		out[id] = r
	}
	return out
}
