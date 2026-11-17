// Package replay implements the dead-letter replay admin command: it
// returns dead deliveries matching a filter to pending.
package replay

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"webhook-notifier/internal/event"
	"webhook-notifier/internal/store"
)

// ParseFlags parses the replay command's arguments into a filter. An empty
// filter would replay every dead delivery and could flood partners, so it
// needs an explicit --all.
func ParseFlags(args []string) (store.ReplayFilter, error) {
	var (
		f                 store.ReplayFilter
		webhook, from, to string
		all               bool
		fs                = flag.NewFlagSet("replay", flag.ContinueOnError)
	)
	fs.StringVar(&f.AccountID, "account", "", "only this account")
	fs.StringVar(&webhook, "webhook", "", "only this webhook ID")
	fs.StringVar(&from, "from", "", "only events at or after this time (RFC 3339)")
	fs.StringVar(&to, "to", "", "only events before this time (RFC 3339)")
	fs.BoolVar(&all, "all", false, "allow replaying with no filter")
	if err := fs.Parse(args); err != nil {
		return store.ReplayFilter{}, err
	}

	if webhook != "" {
		id, err := store.ParseUUID(webhook)
		if err != nil {
			return store.ReplayFilter{}, fmt.Errorf("--webhook: %w", err)
		}
		f.WebhookID = &id
	}
	var err error
	if f.From, err = parseTime(from); err != nil {
		return store.ReplayFilter{}, fmt.Errorf("--from: %w", err)
	}
	if f.To, err = parseTime(to); err != nil {
		return store.ReplayFilter{}, fmt.Errorf("--to: %w", err)
	}
	if !f.From.IsZero() && !f.To.IsZero() && !f.From.Before(f.To) {
		return store.ReplayFilter{}, errors.New("--from must be before --to")
	}
	if f == (store.ReplayFilter{}) && !all {
		return store.ReplayFilter{}, errors.New("no filter given: pass --account, --webhook, --from/--to, or --all to replay every dead delivery")
	}
	return f, nil
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, s)
}

// Run parses args, replays the matching dead deliveries and reports the
// count on out.
func Run(ctx context.Context, st *store.Store, args []string, out io.Writer) error {
	f, err := ParseFlags(args)
	if err != nil {
		return err
	}
	n, err := st.ReplayDead(ctx, f)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "replayed %d dead deliveries\n", n)
	return err
}

// Requeuer puts input dead letters back on the event queue.
type Requeuer interface {
	Requeue(ctx context.Context, f event.RequeueFilter) (int64, error)
}

// ParseRequeueFlags parses requeue-events arguments. Like replay, an empty
// filter needs an explicit --all.
func ParseRequeueFlags(args []string) (event.RequeueFilter, error) {
	var (
		f   event.RequeueFilter
		ids string
		all bool
		fs  = flag.NewFlagSet("requeue-events", flag.ContinueOnError)
	)
	fs.StringVar(&ids, "id", "", "comma-separated event IDs")
	fs.StringVar(&f.ReasonPrefix, "reason-prefix", "", `only dead letters whose reason starts with this ("received", "malformed")`)
	fs.BoolVar(&all, "all", false, "allow requeueing with no filter")
	if err := fs.Parse(args); err != nil {
		return event.RequeueFilter{}, err
	}
	for _, id := range strings.Split(ids, ",") {
		if id = strings.TrimSpace(id); id != "" {
			f.IDs = append(f.IDs, id)
		}
	}
	if len(f.IDs) == 0 && f.ReasonPrefix == "" && !all {
		return event.RequeueFilter{}, errors.New("no filter given: pass --id, --reason-prefix, or --all to requeue every input dead letter")
	}
	return f, nil
}

// RunRequeue parses args, requeues the matching input dead letters and
// reports the count on out.
func RunRequeue(ctx context.Context, q Requeuer, args []string, out io.Writer) error {
	f, err := ParseRequeueFlags(args)
	if err != nil {
		return err
	}
	n, err := q.Requeue(ctx, f)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "requeued %d events\n", n)
	return err
}
