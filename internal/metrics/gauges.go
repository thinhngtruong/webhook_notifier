package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Backlog is the pipeline-wide state read from the database.
type Backlog struct {
	Pending        int64         // pending deliveries
	ActiveAccounts int64         // accounts in the fair ring
	OldestDueAge   time.Duration // how long the oldest due delivery has waited
	Dead           int64         // dead-letter queue size
}

// Sources supplies the values of the observed gauges at scrape time. Nil
// fields are not observed.
type Sources struct {
	Backlog          func(ctx context.Context) (Backlog, error)
	WorkerSlotsInUse func() int64
	DBPoolInUse      func() int64
	OpenCircuits     func() []string // hosts whose circuit is open
	InputQueue       func(ctx context.Context) (waiting int64, oldestAge time.Duration, err error)
}

// Observe registers the gauges read from s on every scrape. The backlog
// gauges share one callback, so a scrape costs one database query.
func (m *Metrics) Observe(s Sources) error {
	if m == nil {
		return nil
	}
	if s.Backlog != nil {
		pending, err := m.meter.Int64ObservableGauge("webhook.pending_deliveries",
			metric.WithDescription("Pending deliveries (due or scheduled for retry)."))
		if err != nil {
			return err
		}
		active, err := m.meter.Int64ObservableGauge("webhook.active_accounts",
			metric.WithDescription("Accounts with pending work in the fair ring."))
		if err != nil {
			return err
		}
		dead, err := m.meter.Int64ObservableGauge("webhook.dead_deliveries",
			metric.WithDescription("Dead deliveries not replayed: the dead-letter queue."))
		if err != nil {
			return err
		}
		oldest, err := m.meter.Float64ObservableGauge("webhook.oldest_due_pending_age", metric.WithUnit("s"),
			metric.WithDescription("How long the oldest due delivery has been waiting."))
		if err != nil {
			return err
		}
		_, err = m.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			b, err := s.Backlog(ctx)
			if err != nil {
				m.Error(ctx, "metrics")
				return nil // skip this scrape's values; the scrape itself still succeeds
			}
			o.ObserveInt64(pending, b.Pending)
			o.ObserveInt64(active, b.ActiveAccounts)
			o.ObserveInt64(dead, b.Dead)
			o.ObserveFloat64(oldest, b.OldestDueAge.Seconds())
			return nil
		}, pending, active, dead, oldest)
		if err != nil {
			return err
		}
	}
	if s.InputQueue != nil {
		waiting, err := m.meter.Int64ObservableGauge("webhook.input_events_waiting",
			metric.WithDescription("Events in the input queue not yet fanned out."))
		if err != nil {
			return err
		}
		age, err := m.meter.Float64ObservableGauge("webhook.input_oldest_event_age", metric.WithUnit("s"),
			metric.WithDescription("Age of the oldest event in the input queue (ingest falling behind)."))
		if err != nil {
			return err
		}
		if _, err := m.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			n, oldest, err := s.InputQueue(ctx)
			if err != nil {
				m.Error(ctx, "metrics")
				return nil
			}
			o.ObserveInt64(waiting, n)
			o.ObserveFloat64(age, oldest.Seconds())
			return nil
		}, waiting, age); err != nil {
			return err
		}
	}
	if s.WorkerSlotsInUse != nil {
		if _, err := m.meter.Int64ObservableGauge("webhook.worker_slots_in_use",
			metric.WithDescription("Worker slots busy in this replica."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(s.WorkerSlotsInUse())
				return nil
			})); err != nil {
			return err
		}
	}
	if s.DBPoolInUse != nil {
		if _, err := m.meter.Int64ObservableGauge("db.pool_in_use",
			metric.WithDescription("Database connections in use in this replica."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(s.DBPoolInUse())
				return nil
			})); err != nil {
			return err
		}
	}
	if s.OpenCircuits != nil {
		if _, err := m.meter.Int64ObservableGauge("webhook.circuit_open",
			metric.WithDescription("1 for each partner host whose circuit is open in this replica."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				for _, host := range s.OpenCircuits() {
					o.Observe(1, metric.WithAttributes(attribute.String("host", host)))
				}
				return nil
			})); err != nil {
			return err
		}
	}
	return nil
}
