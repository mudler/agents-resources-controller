// Package notify delivers operational events (a tripped watchdog, an
// unhealthy device, a lost worker) to an external sink such as a webhook.
//
// It is deliberately dependency-free of the rest of this module: nothing
// here imports another internal package, so its delivery semantics — retry,
// backoff, drop-on-full-queue, graceful close — can be tested in isolation
// from the controller that will eventually call it.
//
// The property every caller depends on is that Notify never blocks and
// never fails its caller. Notify is meant to be called from the scheduler's
// hot path, from the reaper's sweep, and from HTTP handlers; a wedged or
// unreachable webhook must never stall a job, a device release, or a sweep.
// When the delivery queue is full, the event is dropped and the drop is
// counted, not blocked on.
package notify

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Kind names the class of event being reported. Values are part of the
// public wire format documented for consumers of the webhook sink.
type Kind string

const (
	KindWatchdogTrip    Kind = "watchdog_trip"
	KindDeviceUnhealthy Kind = "device_unhealthy"
	KindWorkerLost      Kind = "worker_lost"
	KindJobLost         Kind = "job_lost"
	KindVerifyFailed    Kind = "verify_failed"
	KindLeaseExpired    Kind = "lease_expired"
)

// Event is one notification. It is the payload a Sink delivers, and its
// JSON tags are a public wire format: a later task documents this shape in
// the README, so field names and tags must not change casually.
type Event struct {
	Kind   Kind      `json:"event"`
	Device string    `json:"device"`
	Job    string    `json:"job"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// Sink delivers a single Event, returning an error if delivery failed.
//
// Deliver MUST respect ctx: it must stop trying and return once ctx is
// done. This is not a nicety — Close cancels the shared delivery context
// and then unconditionally waits for the background goroutine to exit, so
// a Sink that ignores ctx turns a bounded Close into one that hangs until
// the Sink itself returns, regardless of the caller's own deadline. The
// webhook Sink in this package honours ctx via http.NewRequestWithContext;
// any other Sink implementation must do the same.
type Sink interface {
	Deliver(ctx context.Context, e Event) error
}

// Options configures a Notifier.
type Options struct {
	// QueueSize bounds how many events may be buffered awaiting delivery.
	// Once full, Notify drops new events rather than blocking its caller.
	QueueSize int
	// Attempts is the number of delivery attempts made per event before
	// giving up on it. Must be at least 1.
	Attempts int
	// Backoff is the delay before the second attempt; it doubles between
	// each subsequent attempt.
	Backoff time.Duration
}

// Notifier delivers Events to a Sink through a bounded, buffered queue
// served by a single background goroutine. The zero value is not usable;
// construct one with New. A nil *Notifier is explicitly supported by every
// method here — it is the "no webhook configured" deployment, which is the
// default, so call sites never need a nil check before calling Notify or
// Close.
type Notifier struct {
	sink Sink
	opts Options

	// queue is never closed: Notify is called concurrently from many
	// goroutines (scheduler, sweep, HTTP handlers), and closing a channel
	// with concurrent senders is a send-on-closed-channel panic waiting to
	// happen. Shutdown is instead signalled by closing closeRequested, a
	// channel only Close ever touches.
	queue          chan Event
	closeRequested chan struct{}
	closeOnce      sync.Once
	done           chan struct{}

	dropped atomic.Int64

	// deliveryCtx bounds every call to sink.Deliver, for the whole life of
	// the Notifier. It is only ever cancelled by Close, once the caller's
	// own context is done, so that a Close whose context is already
	// cancelled (or that times out while draining) unsticks any in-flight
	// or queued delivery instead of leaving the background goroutine
	// running forever behind a wedged sink.
	deliveryCtx context.Context
	cancel      context.CancelFunc
}

// New creates a Notifier delivering events to sink, and starts the single
// background goroutine that serves its queue. Callers must eventually call
// Close to release that goroutine.
//
// If sink is nil, New returns nil instead of a Notifier that would crash
// its background goroutine on the first delivery attempt. This matters
// because a later task constructs the notifier conditionally on whether a
// webhook is configured — exactly the shape that would otherwise hand New
// a nil Sink. A nil *Notifier is already the documented "no webhook
// configured" state that every method here handles safely, so the caller
// gets correct behaviour for free instead of a crash.
func New(sink Sink, opts Options) *Notifier {
	if sink == nil {
		return nil
	}
	if opts.Attempts < 1 {
		opts.Attempts = 1
	}
	if opts.QueueSize < 1 {
		opts.QueueSize = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Notifier{
		sink:           sink,
		opts:           opts,
		queue:          make(chan Event, opts.QueueSize),
		closeRequested: make(chan struct{}),
		done:           make(chan struct{}),
		deliveryCtx:    ctx,
		cancel:         cancel,
	}
	go n.run()
	return n
}

// Notify enqueues e for delivery and returns immediately. If the queue is
// full, or the Notifier is nil, closed, or otherwise unable to accept e, the
// event is dropped and the drop is counted — Notify must never block its
// caller and must never panic, since it is called from the scheduler's hot
// path, the reaper's sweep, and HTTP handlers.
//
// If e.At is the zero time, Notify stamps it with time.Now().UTC() before
// enqueueing. At is part of the documented wire format an operator's
// consumer may sort or dedupe on, so it must never ship as the Go zero
// value just because a caller forgot to set it.
//
// Note on shutdown: the check for "has Close been called" and the send to
// the queue are two separate steps, not one atomic operation. An event
// whose Notify call is in flight at the exact moment Close runs can lose
// the race after passing the closed check but before its send is received
// by drainRemaining — that event is silently lost and NOT counted in
// Dropped, unlike every other drop path. This is a deliberate accepted gap,
// not a bug: it is bounded by QueueSize, only possible in the brief shutdown
// window, and closing it would mean either blocking Notify (which this
// package exists to avoid) or closing a channel with concurrent senders
// (a panic risk we specifically designed around, see the queue field
// comment above).
func (n *Notifier) Notify(e Event) {
	if n == nil {
		return
	}
	select {
	case <-n.closeRequested:
		n.dropped.Add(1)
		return
	default:
	}
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	select {
	case n.queue <- e:
	default:
		n.dropped.Add(1)
		slog.Warn("notify: dropped event, queue full",
			"event", e.Kind, "device", e.Device)
	}
}

// Dropped reports how many events have been dropped so far because the
// queue was full or because Notify was called after Close. Safe on a nil
// receiver, returning 0.
//
// Only the full-queue case is logged (in Notify, at the point of the drop);
// a post-Close drop is counted here but not logged, since by then the
// Notifier is shutting down and there is no delivery goroutine left to
// usefully report through. Also see Notify's doc comment for the one drop
// path this counter cannot see at all: an event that loses its race against
// a concurrent Close.
func (n *Notifier) Dropped() int64 {
	if n == nil {
		return 0
	}
	return n.dropped.Load()
}

// Close stops accepting new events and drains and attempts to deliver
// whatever is already queued. It is safe to call on a nil receiver and safe
// to call more than once.
//
// When ctx is done (including already-cancelled), Close cancels the shared
// delivery context, which unsticks any pending backoff sleep and any
// in-flight or subsequent Sink.Deliver call that itself honours ctx
// cancellation as required by the Sink contract. Close then unconditionally
// waits for the background goroutine to exit before returning, so the
// goroutine is never leaked — but this means ctx only bounds how long Close
// waits when the Sink cooperates. A Sink that ignores its context can still
// make Close block past ctx's deadline, because Close will not return while
// the goroutine is still running a Deliver call. "Bounded by ctx" is
// therefore a best-effort promise contingent on the Sink, not a hard
// guarantee independent of it.
func (n *Notifier) Close(ctx context.Context) error {
	if n == nil {
		return nil
	}
	n.closeOnce.Do(func() {
		close(n.closeRequested)
	})
	select {
	case <-n.done:
		return nil
	case <-ctx.Done():
		n.cancel()
		<-n.done
		return ctx.Err()
	}
}

// run is the single goroutine that serves the queue for the lifetime of the
// Notifier. It exits once Close signals shutdown and the queue has been
// drained as best-effort.
func (n *Notifier) run() {
	defer close(n.done)
	defer n.cancel()
	for {
		// Shutdown takes priority over an ordinary queue receive. Without
		// this non-blocking probe first, the select below would pick
		// uniformly at random between "another item is ready" and
		// "shutdown was requested" whenever both are true, which means
		// whether the tail of the queue drains through this loop's normal
		// per-item path or through drainRemaining is scheduler luck, not a
		// guarantee — a poor property for the one path that runs when an
		// operator is trying to stop the controller cleanly, and one that
		// made this behavior effectively untestable from outside the
		// package (the racing select took the ordinary path almost every
		// time). The observable outcome is unchanged either way: every
		// queued event still gets delivered on the way out, because
		// drainRemaining drains exactly what this loop would have.
		select {
		case <-n.closeRequested:
			n.drainRemaining()
			return
		default:
		}
		select {
		case e := <-n.queue:
			n.deliver(e)
		case <-n.closeRequested:
			n.drainRemaining()
			return
		}
	}
}

// drainRemaining delivers whatever is already buffered in the queue at the
// moment Close was called, without waiting for more to arrive.
func (n *Notifier) drainRemaining() {
	for {
		select {
		case e := <-n.queue:
			n.deliver(e)
		default:
			return
		}
	}
}

// deliver attempts to hand e to the sink, retrying up to opts.Attempts times
// with doubling backoff between tries. It gives up (after logging) once the
// budget is spent, or as soon as the Notifier's delivery context is
// cancelled — a dropped notification must never propagate back into the
// scheduling path that produced it, and a cancelled Close must not be held
// up waiting out a retry budget.
func (n *Notifier) deliver(e Event) {
	backoff := n.opts.Backoff
	var lastErr error
	made := 0 // number of Sink.Deliver calls actually made, for the give-up log below
	for attempt := 1; attempt <= n.opts.Attempts; attempt++ {
		if err := n.deliveryCtx.Err(); err != nil {
			lastErr = err
			break
		}
		made++
		err := n.sink.Deliver(n.deliveryCtx, e)
		if err == nil {
			return
		}
		lastErr = err
		if attempt < n.opts.Attempts && backoff > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-n.deliveryCtx.Done():
				timer.Stop()
				lastErr = n.deliveryCtx.Err()
				attempt = n.opts.Attempts // stop retrying
			}
			backoff *= 2
		}
	}
	slog.Warn("notify: gave up delivering event",
		"event", e.Kind, "device", e.Device, "job", e.Job, "attempts", made, "error", lastErr)
}
