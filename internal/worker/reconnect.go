package worker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// controllerInstanceHeader is the header the controller stamps its process
// identity on. It MIRRORS server.ControllerInstanceHeader rather than
// importing it, the same way registerRequest mirrors server.RegisterRequest:
// nothing in this package depends on the controller's implementation, only on
// its wire protocol. A test in this package holds the two together.
const controllerInstanceHeader = "Rc-Controller-Instance"

// The reconnection thresholds. All three are vars for the same reason
// startupReleaseBudget is: a test cannot wait out five minutes of silence to
// prove what happens after it. Nothing but a test assigns to them.
//
//   - reregisterAfterSilence is how long this worker must have been out of
//     contact before regaining it counts as a reconnection worth registering
//     again for. It mirrors the controller's sweepUnhealthyAfter
//     (internal/cli/serve.go), which is the horizon past which a silent
//     worker's devices are quarantined — the exact damage only a
//     registration can repair, since only a registration carries the proof
//     (a boot ID, a survivor scan) that clears one. Below it the worst
//     silence costs is a demotion to unknown, and the next heartbeat undoes
//     that on its own. That package cannot be imported here (it is what
//     builds the worker command), so the value is mirrored, the way
//     internal/cli mirrors the store's quarantine reasons.
//
//   - reregisterJitter spreads a fleet's reconnections. When a controller
//     comes back, every worker on it is armed within milliseconds of every
//     other one; each waits its own draw from this window before registering,
//     so a hundred-node fleet does not open a hundred registrations — the
//     most expensive request this API serves — in the same instant on a
//     controller that has just started.
//
//   - reregisterRetryAfter paces a re-registration that failed. It is still
//     owed (the device it would prove clean is still out of the pool), but a
//     poll loop spinning at its floor interval would otherwise retry it
//     several times a second against a controller that is evidently already
//     unwell.
var (
	reregisterAfterSilence = 5 * time.Minute
	reregisterJitter       = 30 * time.Second
	reregisterRetryAfter   = time.Minute
)

// contactTracker decides when this worker should register again.
//
// Registration is the only message that carries what the controller needs to
// return a quarantined device to the pool — the boot ID and the recovery
// proof (see store.AutoRecover) — and until 2026-08-18 the worker sent it
// exactly once, at startup. That covered a worker restart and nothing else.
// When the CONTROLLER restarted instead, the worker carried on heartbeating
// against the new process, never presented proof to it, and the device it
// could have cleared stayed quarantined until a human cleared it by hand.
//
// So the worker registers again on either of the two things it can actually
// observe:
//
//   - the controller it is talking to is a different process than the one it
//     registered with (the instance identity changed), or
//   - it was out of contact for long enough that the controller may have
//     written its devices off, and contact is now back.
//
// Both are edge-triggered and both are deliberately narrow. Neither fires on
// a single failed request, because registration is not free: the controller
// treats it as a fresh process announcing it has no running jobs, and reaps
// whatever it believes is in flight for this worker. Re-registering on a blip
// would cause exactly the damage this exists to repair — see maybeReregister
// for the second half of that guard, which is that a worker with a job in
// hand never registers at all.
//
// It is not a proof channel and carries no authority of its own: everything
// it can do is ask for the ordinary registration path to run, whose ordinary
// rules — proof or no recovery, fault never cleared — decide what actually
// happens.
type contactTracker struct {
	mu sync.Mutex
	// jitter draws this worker's delay before acting on an armed
	// reconnection. A field, not a call to rand, so a test can pin it.
	jitter func() time.Duration
	// lastContact is when an exchange with the controller last completed.
	// Zero until the first one, which is why a fresh worker's first contact
	// arms nothing: there is no gap to measure.
	lastContact time.Time
	// instance is the controller process last seen. Zero until one names
	// itself; an unnamed response (an older controller, a header-stripping
	// proxy) never overwrites a name we already have, and is never read as a
	// change.
	instance string
	// dueAt is when the owed re-registration may happen. Zero means none is
	// owed.
	dueAt time.Time
}

func newContactTracker() *contactTracker {
	return &contactTracker{jitter: func() time.Duration {
		if reregisterJitter <= 0 {
			return 0
		}
		return rand.N(reregisterJitter)
	}}
}

// observe records one completed exchange with the controller. "Completed"
// means the controller answered at all — a 500 is contact, a refused
// connection is not — because what is being measured here is reachability,
// not success.
func (t *contactTracker) observe(now time.Time, instance string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.lastContact.IsZero() && now.Sub(t.lastContact) >= reregisterAfterSilence {
		t.arm(now)
	}
	if instance != "" {
		if t.instance != "" && instance != t.instance {
			t.arm(now)
		}
		t.instance = instance
	}
	// Only ever moved forward. Exchanges finish out of order — a long poll
	// started before a heartbeat can answer after it — and a gap measured
	// from a stale timestamp would arm on silence that never happened.
	if now.After(t.lastContact) {
		t.lastContact = now
	}
}

// arm schedules a re-registration, unless one is already owed. A second
// reason to register does not make it more owed, and must not push the
// existing appointment around: the worker would keep deferring the very
// registration it needs.
//
// The caller holds t.mu.
func (t *contactTracker) arm(now time.Time) {
	if !t.dueAt.IsZero() {
		return
	}
	t.dueAt = now.Add(t.jitter())
}

// due reports whether a re-registration is owed and its delay has elapsed.
func (t *contactTracker) due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.dueAt.IsZero() && !now.Before(t.dueAt)
}

// attempting records that a re-registration is being made now. The debt is
// NOT cleared — only registered() does that — so an attempt that fails is
// tried again, but not before reregisterRetryAfter.
func (t *contactTracker) attempting(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dueAt = now.Add(reregisterRetryAfter)
}

// registered settles the debt: the controller has this worker's proof, and
// nothing further is owed until contact is lost or the controller changes
// again.
func (t *contactTracker) registered() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dueAt = time.Time{}
}

// maybeReregister registers again if the tracker says one is owed and this
// worker is in a state where registering is safe.
//
// The safety condition is that this worker is supervising nothing, and it is
// not a nicety. UpsertWorker treats every registration as a fresh process
// announcing it has no running jobs: it marks whatever it believes is in
// flight for this worker as lost, releases those leases and quarantines those
// devices with reason "registration". Registering while a job is running here
// would therefore kill that job and quarantine the GPU under it — the exact
// damage this whole change exists to stop the controller doing.
//
// The same condition is also the only one in which registering can achieve
// anything. The proof a registration carries is a scan for processes tagged
// with RC_JOB_ID (see recoveryProof); a worker running a job has such
// processes, so the scan would honestly report a survivor and the controller
// would correctly recover nothing. Waiting until the job ends costs the
// device nothing — it is in the pool, being used — and turns an unprovable
// claim into a provable one.
//
// The residual window, stated rather than papered over: a job could be
// assigned to this worker between the poll that came back empty and the
// registration arriving, and would then be reaped as lost without ever
// running. That is the same reap a genuine worker restart performs, it is
// bounded by one request round trip, and the alternative — teaching
// registration to trust a worker's own account of what it is running — would
// undo the reconciliation that registration exists for.
func (w *Worker) maybeReregister(ctx context.Context) {
	if !w.contact.due(time.Now()) {
		return
	}
	if running := w.supervisedJobIDs(); len(running) > 0 {
		// Not logged: this is checked on every idle poll, and a worker with
		// a long job would print it for hours.
		return
	}
	w.contact.attempting(time.Now())

	slog.Info("registering again after losing contact with the controller, so it can re-evaluate this host's quarantined devices",
		"host", w.cfg.Host)
	if err := w.register(ctx); err != nil {
		// Warn, not error: the devices this would have recovered stay
		// quarantined, which is the safe direction and today's behaviour.
		slog.Warn("re-registration failed; this host's quarantined devices stay out of the pool until the next attempt",
			"host", w.cfg.Host, "retry_in", reregisterRetryAfter, "err", err)
		return
	}
	w.contact.registered()
}
