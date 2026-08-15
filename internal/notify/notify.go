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
// Deliver must respect ctx: when ctx is done, Deliver should stop trying
// and return promptly.
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
func New(sink Sink, opts Options) *Notifier {
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
	select {
	case n.queue <- e:
	default:
		n.dropped.Add(1)
		slog.Warn("notify: dropped event, queue full",
			"event", e.Kind, "device", e.Device)
	}
}

// Dropped reports how many events have been dropped so far because the
// queue was full. Safe on a nil receiver, returning 0.
func (n *Notifier) Dropped() int64 {
	if n == nil {
		return 0
	}
	return n.dropped.Load()
}

// Close stops accepting new events, drains and attempts to deliver whatever
// is already queued (bounded by ctx), and waits for the background
// goroutine to finish. It is safe to call on a nil receiver, safe to call
// more than once, and returns promptly even when ctx is already cancelled:
// in that case it cancels delivery immediately so the background goroutine
// unwinds without waiting out any pending backoff or a wedged sink, and
// still waits for that goroutine to actually finish before returning, so
// the goroutine is never leaked.
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
	for attempt := 1; attempt <= n.opts.Attempts; attempt++ {
		if err := n.deliveryCtx.Err(); err != nil {
			lastErr = err
			break
		}
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
		"event", e.Kind, "device", e.Device, "attempts", n.opts.Attempts, "error", lastErr)
}
