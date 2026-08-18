package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/notify"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/spf13/cobra"
)

// HeartbeatGrace is how long a device's worker may go without heartbeating
// before it is treated as out of contact. It gates both the reaper's Sweep
// threshold below and rc devices' "no contact" annotation (internal/cli/ps.go)
// so the two thresholds cannot silently drift apart.
const HeartbeatGrace = 30 * time.Second

// notifierOptions bounds what a webhook outage can cost. The queue is deep
// enough to hold a bad minute's worth of events (a fleet-wide sweep that
// loses every device at once is tens of events, not hundreds) and shallow
// enough that a wedged endpoint sheds load instead of growing without limit.
//
// Attempts and Backoff are deliberately small. Delivery is serialised
// through one goroutine, so every retry of one event delays every event
// behind it; three attempts against an endpoint that accepts connections and
// never answers already costs ~30s of that goroutine's time (the webhook
// sink's own per-request timeout), which is the most a queued notification
// should ever be worth.
var notifierOptions = notify.Options{QueueSize: 256, Attempts: 3, Backoff: time.Second}

// notifierCloseTimeout bounds shutdown. Close drains and delivers what is
// still queued, which against a dead endpoint would otherwise cost
// Attempts × the per-request timeout, plus backoff, for EVERY queued event
// in turn — many minutes of an operator waiting for a controller to stop.
// Past this deadline Close cancels the in-flight delivery and gives up on
// the rest: a notification is never worth holding a shutdown open for.
const notifierCloseTimeout = 5 * time.Second

// sweepInterval is how often the reaper runs, and sweepUnhealthyAfter how
// long a worker may be silent before its devices are written off entirely
// (as opposed to merely demoted to unknown after HeartbeatGrace).
//
// They are vars rather than consts only so a test can drive the reaper's own
// goroutine without waiting out ten seconds of real time or five minutes of
// silence — the loop reads them once per iteration, and nothing but a test
// ever assigns to them. That seam is worth having: it is the only way to
// prove from outside that the sweep's events actually reach a configured
// webhook, rather than that a function which would have emitted them exists.
var (
	sweepInterval       = 10 * time.Second
	sweepUnhealthyAfter = 5 * time.Minute
)

// startupGrace is how long after this process starts the reaper declines to
// draw a destructive conclusion from a stored timestamp — it expires no lease
// and writes off no silent worker until the window has passed. See
// store.Sweep's holdOffUntil, which is where the rule itself lives.
//
// On 2026-08-18 the controller was restarted to pick up a new image. It was
// down for a few seconds, and its first sweep expired a lease whose deadline
// had lapsed during exactly that gap: the job was running perfectly well, its
// holder had simply had no controller to renew against. The scheduler
// restarting killed the work.
//
// The value is HeartbeatGrace, and it is the same number for the same reason:
// HeartbeatGrace is this controller's own definition of how long a worker may
// be quiet before we treat it as out of contact, and a worker heartbeats
// every 10s by default (worker.defaultHeartbeatInterval), so it is three
// heartbeats' worth of slack. Below it the controller has concluded nothing
// about a worker, so it must not conclude anything about that worker's lease
// either — a sweep that would expire a lease inside a window in which it does
// not even consider the holder out of contact is contradicting itself. One
// renewal is all a live holder needs, and it gets three chances at it.
//
// It is deliberately not a flag. An operator has no information with which to
// tune it: it is a function of the heartbeat interval, which the worker
// already owns, and every value that is not "long enough for one heartbeat"
// is wrong in one direction or the other. It is a var for the same single
// reason sweepInterval and sweepUnhealthyAfter are: a test driving the
// reaper's own goroutine cannot wait out thirty seconds of real time to see
// it write anything off. Nothing but a test ever assigns to it.
var startupGrace = HeartbeatGrace

func NewServeCmd() *cobra.Command {
	var (
		addr       string
		dataDir    string
		webhookURL string
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the controller",
		RunE: func(cmd *cobra.Command, args []string) error {
			tokens, err := loadTokens()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				return err
			}

			c := clock.Real()
			st, err := store.Open(filepath.Join(dataDir, "rc.db"), c)
			if err != nil {
				return err
			}
			defer st.Close()

			logs, err := logstore.New(filepath.Join(dataDir, "logs"))
			if err != nil {
				return err
			}

			// Built unconditionally: with no webhook configured webhookSink
			// returns a nil Sink, notify.New answers with a nil *Notifier,
			// and every method on that is a safe no-op. So no call site
			// downstream — handler, sweep or shutdown — needs to know
			// whether an operator configured a webhook at all.
			hook := resolveWebhookURL(webhookURL)
			notifier := notify.New(webhookSink(hook), notifierOptions)
			if hook != "" {
				slog.Info("event webhook configured", "url", hook)
			}

			srv := server.New(server.Config{
				Store: st, Logs: logs, Clock: c, Tokens: tokens, Notifier: notifier,
			})

			// The reaper: silent workers lose their devices to unknown, then
			// unhealthy. Nothing here ever promotes a device to ready.
			//
			// It runs on its own derived context, not cmd.Context() directly:
			// RunE can return along a path that never cancels cmd.Context() at
			// all — e.g. ListenAndServe failing immediately on a bind error —
			// and reaperWG.Wait() below must not block forever waiting for a
			// cancellation that will never come. cancelReaper is deferred so it
			// fires on every return path; reaperWG.Wait() is joined before
			// st.Close() runs (see the deferred order below) so the reaper can
			// never touch the store after it's closed and log a spurious
			// "sql: database is closed".
			reaperCtx, cancelReaper := context.WithCancel(cmd.Context())

			// Stamped once, here, rather than recomputed per tick: the
			// window is "how long since this controller started", and a
			// value the loop derived itself each time would silently become
			// "how long since the last tick".
			holdOffUntil := c.Now().Add(startupGrace)

			var reaperWG sync.WaitGroup
			reaperWG.Add(1)
			go func() {
				defer reaperWG.Done()
				t := time.NewTicker(sweepInterval)
				defer t.Stop()
				for {
					select {
					case <-reaperCtx.Done():
						return
					case <-t.C:
						res, err := sweepAndNotify(st, notifier, HeartbeatGrace, sweepUnhealthyAfter, holdOffUntil)
						if err != nil {
							slog.Error("sweep", "err", err)
							continue
						}
						if len(res.DevicesUnhealthy) > 0 || len(res.JobsLost) > 0 || len(res.LeasesExpired) > 0 {
							slog.Warn("devices demoted",
								"unhealthy", res.DevicesUnhealthy, "jobs_lost", res.JobsLost,
								"leases_expired", res.LeasesExpired)
							srv.Publish("devices", nil)
						}
					}
				}
			}()
			// The scheduler: assigns queued jobs to devices as they free up.
			// handleSubmit already makes one scheduling pass at submit time so
			// a free device starts immediately; this loop is what notices a
			// device freeing up later (a job finishing, a worker
			// re-registering) and hands the next queued job to it. It runs on
			// the same reaperCtx and is joined the same way, for the same
			// reason: reaperCtx.Done() is guaranteed to fire on every return
			// path, and the store must not be touched after st.Close() runs
			// below.
			var schedulerWG sync.WaitGroup
			schedulerWG.Add(1)
			go func() {
				defer schedulerWG.Done()
				t := time.NewTicker(time.Second)
				defer t.Stop()
				for {
					select {
					case <-reaperCtx.Done():
						return
					case <-t.C:
						assigned, err := st.ScheduleOnce()
						if err != nil {
							slog.Error("schedule", "err", err)
							continue
						}
						for _, job := range assigned {
							srv.Poke(job.WorkerID)
						}
						if len(assigned) > 0 {
							srv.Publish("jobs", nil)
						}
					}
				}
			}()

			// Deferred in this order so Go's LIFO unwind runs them
			// cancelReaper -> reaperWG.Wait -> schedulerWG.Wait -> st.Close:
			// stop both loops first (works even if cmd.Context() is still
			// live), then wait for them to actually exit, then close the
			// store they were using.
			defer reaperWG.Wait()
			defer schedulerWG.Wait()
			defer cancelReaper()

			// httpWG joins the HTTP server's own shutdown, which is the
			// third emitter and the one nothing used to wait for. The
			// request handlers emit too (a fault, a watchdog trip), and
			// ListenAndServe returns the moment the listener closes, NOT
			// when handlers finish — so without this join RunE could unwind
			// while a handler was still mid-request. A fault POST caught in
			// that window answered 200 while its event was dropped by an
			// already-closed notifier, and the operator was never told a
			// device had left the pool. The same window could also hand a
			// live handler a database st.Close had already closed.
			var httpWG sync.WaitGroup

			// Draining the notifier is the last thing that happens, and it
			// does its own stopping and joining rather than relying on where
			// it sits in the defer stack. Order matters here — an event
			// emitted by anything still running after the queue has been
			// drained is dropped silently, at exactly the moment an operator
			// most wants to know what happened — and a property this
			// load-bearing should not rest on a reader noticing that LIFO
			// unwinding puts this defer in the right place. Every call below
			// is idempotent, so the outer defers repeating them costs
			// nothing.
			defer func() {
				httpWG.Wait()
				cancelReaper()
				reaperWG.Wait()
				schedulerWG.Wait()

				ctx, cancel := context.WithTimeout(context.Background(), notifierCloseTimeout)
				defer cancel()
				if err := notifier.Close(ctx); err != nil {
					slog.Warn("gave up delivering queued notifications", "err", err)
				}
			}()

			httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}
			// serveOver releases the shutdown goroutine when ListenAndServe
			// returned on its own. That is not a nicety either: RunE can
			// return on a path that never cancels cmd.Context() — a bind
			// failure being the one that matters — and httpWG.Wait() above
			// would otherwise wait for a cancellation that never comes,
			// turning a port conflict back into a hang.
			serveOver := make(chan struct{})
			httpWG.Add(1)
			go func() {
				defer httpWG.Done()
				select {
				case <-cmd.Context().Done():
				case <-serveOver:
					return
				}
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				// Shutdown waits for in-flight handlers to return, bounded
				// by shutdownCtx. That wait is the whole value of the join.
				_ = httpSrv.Shutdown(shutdownCtx)
			}()

			slog.Info("controller listening", "addr", addr, "data", dataDir)
			serveErr := httpSrv.ListenAndServe()
			close(serveOver)
			if serveErr != nil && serveErr != http.ErrServerClosed {
				return serveErr
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	cmd.Flags().StringVar(&dataDir, "data", "/var/lib/rc", "state directory")
	// The six kinds are listed in full, and in the same order the README's
	// table uses: an operator deciding whether to wire this up should not
	// have to discover from the README that the two most specific kinds —
	// the ones a verify script and a lapsed lease produce — exist at all.
	// notify.Kind's constants are the source of truth; if a seventh is ever
	// added, this string and the README table are what must follow it.
	cmd.Flags().StringVar(&webhookURL, "webhook-url", "",
		"POST operational events to this URL as JSON: watchdog_trip, verify_failed, "+
			"device_unhealthy, worker_lost, job_lost, lease_expired; defaults to $RC_WEBHOOK_URL")
	return cmd
}

// resolveWebhookURL picks the webhook endpoint: the flag if it was given,
// otherwise RC_WEBHOOK_URL, otherwise none. The flag wins so an operator can
// override an environment set by a unit file without editing the unit.
func resolveWebhookURL(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv("RC_WEBHOOK_URL")
}

// webhookSink returns the Sink for url, or a nil Sink when no webhook is
// configured. Returning nil rather than a Sink pointed at "" is what makes
// notify.New hand back a nil *Notifier, which is the documented "no webhook"
// state — see the call site in RunE.
func webhookSink(url string) notify.Sink {
	if url == "" {
		return nil
	}
	return notify.NewWebhook(url, nil)
}

// sweepEvents maps one sweep's outcome to the events it should announce.
//
// It is deliberately given only what the sweep actually reports —
// store.SweepResult carries device and job IDs, nothing else — plus the
// quarantine reasons read back afterwards. The events are correspondingly
// modest: job_lost carries the exact kill_reason the reaper wrote ("worker
// lost") and no device, because the sweep never told us which device the job
// was on.
//
// The worker_lost / device_unhealthy split is decided by the stored
// quarantine reason rather than by anything in SweepResult, which cannot
// tell the two apart. In practice a device that newly turns unhealthy in a
// sweep was quarantined by this very sweep for worker loss; the general
// branch covers a reason written by another path in between, and a reason
// nobody recorded, neither of which should be reported as a lost worker on
// no evidence.
//
// An expired lease quarantines its device too, and SweepResult does not
// report that at all — LeasesExpired holds job (or lease) IDs, and
// DevicesUnhealthy only ever holds devices demoted for worker silence. So
// that device is recovered here, from the lease row, and announced: a device
// silently leaving the pool is the one thing an event stream about vanishing
// capacity must never omit. It is reported only when the device's CURRENT
// quarantine reason is the expiry itself; a device already out of the pool
// for another cause keeps that cause and was already announced when it
// happened.
func sweepEvents(res store.SweepResult, reasons, leaseDevices map[string]string) []notify.Event {
	// Devices demoted to unknown are absent on purpose: unknown is not a
	// quarantine, and the next heartbeat routinely undoes it.
	out := make([]notify.Event, 0, len(res.DevicesUnhealthy)+len(res.JobsLost)+len(res.LeasesExpired))
	// announced guards against reporting one device twice in a single
	// sweep: a device can lose its worker and have its lease expire in the
	// same pass, and two events for one quarantine would make any consumer
	// counting unhealthy devices overstate the damage.
	announced := make(map[string]bool, len(res.DevicesUnhealthy))
	for _, id := range res.DevicesUnhealthy {
		reason := reasons[id]
		kind := notify.KindDeviceUnhealthy
		if reason == quarantineWorkerLost {
			kind = notify.KindWorkerLost
		}
		announced[id] = true
		out = append(out, notify.Event{Kind: kind, Device: id, Reason: reason})
	}
	for _, id := range res.JobsLost {
		out = append(out, notify.Event{Kind: notify.KindJobLost, Job: id, Reason: sweepJobLostReason})
	}
	for _, id := range res.LeasesExpired {
		device := leaseDevices[id]
		out = append(out, notify.Event{
			Kind: notify.KindLeaseExpired, Job: id, Device: device, Reason: sweepLeaseExpiredReason,
		})
		if device == "" || announced[device] {
			continue
		}
		announced[device] = true
		if reasons[device] == quarantineLeaseExpired {
			out = append(out, notify.Event{
				Kind: notify.KindDeviceUnhealthy, Device: device, Reason: quarantineLeaseExpired,
			})
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

const (
	// quarantineWorkerLost mirrors the constant internal/store/reaper.go
	// stores in devices.quarantine_reason when a sweep writes a worker off.
	// It is unexported there, and it is a stored value in a live database,
	// so it is matched by value here rather than re-exported.
	quarantineWorkerLost = "worker_lost"
	// quarantineLeaseExpired likewise mirrors the reason the reaper stores
	// on a device whose lease lapsed with nobody renewing it.
	quarantineLeaseExpired = "lease_expired"
	// sweepJobLostReason and sweepLeaseExpiredReason are the exact
	// kill_reason strings the reaper records on the jobs it ends, so an
	// operator reading `rc ps` sees the same words the webhook told them.
	sweepJobLostReason      = "worker lost"
	sweepLeaseExpiredReason = "lease expired"
)

// sweepAndNotify runs one sweep and announces what it changed.
//
// The quarantine reasons are read AFTER Sweep returns, never during it: the
// store runs at one connection, so a second query issued while Sweep's own
// cursors are open would deadlock. Failing to read them is not allowed to
// fail the sweep — the reaper's job is to reclaim hardware, and a
// notification it could not fully label is still worth sending — so the
// events degrade to device_unhealthy instead.
func sweepAndNotify(st *store.Store, n *notify.Notifier, grace, unhealthyAfter time.Duration, holdOffUntil time.Time) (store.SweepResult, error) {
	res, err := st.Sweep(grace, unhealthyAfter, holdOffUntil)
	if err != nil {
		return res, err
	}

	// Two post-commit reads at most, and none at all on the overwhelmingly
	// common sweep that changed nothing.
	var leaseDevices map[string]string
	if len(res.LeasesExpired) > 0 {
		leaseDevices, err = st.LeaseDevices(res.LeasesExpired)
		if err != nil {
			slog.Error("read devices behind expired leases for event notifications", "err", err)
			leaseDevices = nil
		}
	}

	quarantined := make([]string, 0, len(res.DevicesUnhealthy)+len(leaseDevices))
	quarantined = append(quarantined, res.DevicesUnhealthy...)
	for _, id := range res.LeasesExpired {
		if device := leaseDevices[id]; device != "" {
			quarantined = append(quarantined, device)
		}
	}
	var reasons map[string]string
	if len(quarantined) > 0 {
		reasons, err = st.QuarantineReasons(quarantined)
		if err != nil {
			slog.Error("read quarantine reasons for event notifications", "err", err)
			reasons = nil
		}
	}
	for _, e := range sweepEvents(res, reasons, leaseDevices) {
		// Never blocks: a wedged webhook drops events, it does not hold up
		// the reaper.
		n.Notify(e)
	}
	return res, nil
}

// loadTokens reads RC_TOKENS as "token:role,token:role". It rejects anything
// that would make the resulting policy something other than what the
// operator literally wrote: a malformed entry, an unknown role, an empty
// token, or a token repeated with a second (possibly different, possibly
// higher-privileged) role. A silently-dropped or silently-overwritten entry
// is an auth policy nobody actually reviewed.
func loadTokens() (map[string]string, error) {
	raw := os.Getenv("RC_TOKENS")
	if raw == "" {
		return nil, fmt.Errorf("RC_TOKENS required, e.g. RC_TOKENS='wtok:worker,ctok:client,atok:admin'")
	}
	tokens := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		token, role, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok {
			return nil, fmt.Errorf("malformed RC_TOKENS entry %q", pair)
		}
		if token == "" {
			return nil, fmt.Errorf("empty token in RC_TOKENS entry %q", pair)
		}
		switch role {
		case "worker", "client", "admin":
		default:
			return nil, fmt.Errorf("unknown role %q in RC_TOKENS", role)
		}
		if _, dup := tokens[token]; dup {
			return nil, fmt.Errorf("duplicate token %q in RC_TOKENS", token)
		}
		tokens[token] = role
	}
	return tokens, nil
}
