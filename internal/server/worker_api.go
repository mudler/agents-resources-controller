package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/notify"
	"github.com/mudler/resource-controller/internal/store"
)

// maxLogChunk bounds a single log upload. A worker whose chunk exceeds this
// is told so explicitly (413) and must split and retry: silently truncating
// a job's output would lose data without telling anyone.
const maxLogChunk = 1 << 20

// maxHeartbeatBody bounds a heartbeat's job list. A worker supervises a
// handful of jobs at most (one per device), so anything approaching this is
// either a bug or an attempt to make the controller allocate on demand.
const maxHeartbeatBody = 64 << 10

// maxSheetBytes mirrors the worker's own truncation cap (see
// worker.maxSheetBytes) as a second line of defence: the worker already
// truncates before sending, but this rejects outright anything that still
// arrives oversized — a hand-edited file that bypassed truncation, a future
// caller of this API that doesn't — rather than silently accepting it into
// the database.
const maxSheetBytes = 64 * 1024

// DeviceSpec is one device a worker declares at registration: its name and,
// if the operator configured one, the runtime ceiling the controller should
// enforce against any job scheduled onto it.
type DeviceSpec struct {
	Name              string `json:"name"`
	MaxRuntimeSeconds int    `json:"max_runtime_seconds,omitempty"`
}

type RegisterRequest struct {
	Host    string       `json:"host"`
	BootID  string       `json:"boot_id,omitempty"`
	Devices []DeviceSpec `json:"devices"`

	// Labels is this registration's freshly detected device facts: the
	// empty key "" holds host-wide facts merged into every device (a
	// device-scoped value wins any key collision), any other key names one
	// device by its bare name.
	//
	// The field is deliberately NOT json:",omitempty": a nil map (absent
	// from the wire entirely) and a non-nil, empty map (present as {})
	// decode to different Go values, and that difference is the only signal
	// this handler has for telling "the probe pass found nothing at all, so
	// say nothing and leave what's stored alone" apart from "the pass ran
	// and legitimately detected zero labels, so clear what's stored" — see
	// ReplaceLabels's own doc comment for why the latter must replace.
	// worker.labelsPayload is the caller-side half of this contract: it
	// returns nil, not an empty map, when a whole probe pass produced not a
	// single fact, so a fleet-wide probe outage can never wipe every
	// device's detected labels and, with them, every selector that depends
	// on them.
	Labels map[string]map[string]string `json:"labels"`
	// DeclaredLabels is the operator-asserted counterpart to Labels (from
	// worker.yaml's DeviceConfig.Labels), same shape and the same
	// nil-vs-empty contract — though for this source there is no probe to
	// fail: the worker always sends its current config verbatim, so an
	// operator who removes a declared label and restarts actually sees it
	// cleared, the same way an unset max_runtime clears a ceiling.
	DeclaredLabels map[string]map[string]string `json:"declared_labels"`
	// Sheet is this host's usage-sheet documentation (host.md), and
	// DeviceSheets is each device's own (host.d/<device>.md), keyed by bare
	// device name.
	//
	// Sheet is a *string, not a plain string, for the same nil-vs-empty
	// reason Labels is a nil-checked map — a fix-round-1 finding: the
	// worker's readSheets can fail to read host.md for a reason OTHER than
	// it simply not existing (permission denied, ...), and a plain string
	// could not tell that apart from "the host genuinely has no sheet".
	// nil means "leave whatever is already stored for this host's sheet
	// alone"; a non-nil pointer, even to "", is an explicit, trustworthy
	// report and is applied via UpsertHostDoc. DeviceSheets needs no such
	// pointer: a device whose sheet could not be read is simply omitted
	// from the map (its key is absent), which applyDeviceFacts already
	// treats as "leave it alone" via its `if body, ok := ...` check.
	Sheet        *string           `json:"sheet,omitempty"`
	DeviceSheets map[string]string `json:"device_sheets,omitempty"`
}

// LabelsPushRequest is what a worker posts on every probe-interval pass
// AFTER its initial registration: the same detected/declared facts and
// usage sheets RegisterRequest carries, scoped to ONLY that. It deliberately
// has no boot_id or device-upsert path, and handlePushLabels never calls
// UpsertWorker — see that handler's doc comment for why reusing
// registration itself for this would be actively dangerous. Sheet has the
// same nil-vs-empty contract as RegisterRequest.Sheet.
type LabelsPushRequest struct {
	Host           string                       `json:"host"`
	Devices        []string                     `json:"devices"`
	Labels         map[string]map[string]string `json:"labels"`
	DeclaredLabels map[string]map[string]string `json:"declared_labels"`
	Sheet          *string                      `json:"sheet,omitempty"`
	DeviceSheets   map[string]string            `json:"device_sheets,omitempty"`
}

type RegisterResponse struct {
	WorkerID string `json:"worker_id"`
}

type Assignment struct {
	JobID              string            `json:"job_id"`
	DeviceID           string            `json:"device_id"`
	Command            []string          `json:"command"`
	Cwd                string            `json:"cwd,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	MaxRuntimeSeconds  int               `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int               `json:"idle_timeout_seconds,omitempty"`
	// Submitter carries the job's submitter so a lease lifecycle hook's
	// RC_SUBMITTER can name them.
	Submitter string `json:"submitter,omitempty"`
	// Kind is model.LeaseKindJob or model.LeaseKindHold. A worker that
	// sees "hold" substitutes its own sleeper for Command — see
	// internal/worker's execute — rather than trusting what is stored
	// here, which for a hold is a fixed, meaningless placeholder.
	Kind string `json:"kind,omitempty"`
}

// PollResponse is the envelope handleAssignments answers a long-poll with: it
// carries both newly handed-out assignments and job IDs the controller wants
// killed, so a kill reaches the worker as fast as an assignment does rather
// than waiting on a separate channel.
type PollResponse struct {
	Assignments []Assignment `json:"assignments"`
	Kills       []string     `json:"kills,omitempty"`
}

// HeartbeatRequest is what a worker says on every heartbeat: the IDs of the
// jobs it is actually supervising right now. The controller renews the leases
// of exactly those jobs and no others, so a job the worker has no process for
// — one whose assignment response never arrived, say — stops being renewed
// and falls to lease expiry rather than being kept alive forever by an
// unrelated liveness signal.
type HeartbeatRequest struct {
	RunningJobIDs []string `json:"running_job_ids,omitempty"`
}

// FaultRequest is what a worker sends when it has decided a device must
// leave the pool: an on_acquire hook that exited non-zero or timed out, or a
// verify pass that failed after a job (see internal/worker/verify.go).
//
// Reason is free text — the hook's tail output, or the verify pass's
// "verify failed: ..." — and is NOT persisted on the device row, which has a
// fixed quarantine_reason vocabulary (see internal/store/reaper.go). Where
// an operator can actually read it depends on which source produced it, and
// the two differ:
//
//   - a failed hook fails its job too, so the text rides that job's failure
//     report and `rc ps` surfaces it;
//   - a failed verify pass leaves the job SUCCEEDED (the run was fine; the
//     device is not), so there is no failure report to carry it. It reaches
//     the worker's log, this controller's log (see handleDeviceFault), and
//     the verify_failed webhook event — and nowhere a client API returns.
//
// So: do not describe the job's failure report as the operator-facing "why"
// in general. It is only that for the hook case.
type FaultRequest struct {
	Reason string `json:"reason"`
}

type StatusRequest struct {
	WorkerID string         `json:"worker_id"`
	State    model.JobState `json:"state"`
	ExitCode *int           `json:"exit_code,omitempty"`
	Reason   string         `json:"reason,omitempty"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Host == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "host required")
		return
	}
	if name := oversizedSheet(req.Sheet, req.DeviceSheets); name != "" {
		writeErr(w, http.StatusRequestEntityTooLarge, "sheet_too_large",
			fmt.Sprintf("usage sheet %q exceeds 64KB", name))
		return
	}

	now := s.cfg.Clock.Now()
	// The worker ID is derived from the host so a restarted worker resumes its
	// own devices instead of orphaning them.
	workerID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(req.Host)).String()

	devices := make([]model.Device, 0, len(req.Devices))
	for _, dev := range req.Devices {
		devices = append(devices, model.Device{
			ID: req.Host + ":" + dev.Name, Host: req.Host, Name: dev.Name,
			WorkerID: workerID, State: model.DeviceReady, LastHeartbeatAt: now,
		})
	}

	if err := s.cfg.Store.UpsertWorker(
		model.Worker{ID: workerID, Host: req.Host, BootID: req.BootID, LastHeartbeatAt: now}, devices); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	// UpsertWorker creates/updates the device rows themselves but does not
	// touch their runtime ceiling; that is set here, once each device row is
	// known to exist. worker.yaml is the declaration of intent, so
	// registration must make the stored ceiling match it exactly — including
	// when the declaration is "no ceiling": a device that previously
	// declared max_runtime and now omits it must actually be cleared, or an
	// operator who deletes it and restarts sees no change and can only fix
	// it by hand-editing the database. Every device in this registration
	// gets its ceiling written, zero included, not just the ones with a
	// positive value.
	for _, dev := range req.Devices {
		sec := dev.MaxRuntimeSeconds
		if sec < 0 {
			sec = 0
		}
		id := req.Host + ":" + dev.Name
		if err := s.cfg.Store.SetDeviceMaxRuntime(id, time.Duration(sec)*time.Second); err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
	}
	deviceNames := make([]string, 0, len(req.Devices))
	for _, dev := range req.Devices {
		deviceNames = append(deviceNames, dev.Name)
	}
	if err := s.applyDeviceFacts(req.Host, deviceNames, req.Labels, req.DeclaredLabels, req.Sheet, req.DeviceSheets, now); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	s.publishDevices()
	writeJSON(w, http.StatusOK, RegisterResponse{WorkerID: workerID})
}

// handlePushLabels stores a worker's freshly probed device facts and usage
// sheets on the same interval its probes re-run, after the initial
// registration.
//
// This is deliberately a SEPARATE route from handleRegister, not a repeated
// call to it: UpsertWorker treats every registration as a fresh process
// announcing it has no running jobs (see its own doc comment), and reaps
// anything still "assigned" or "running" under that worker ID on the theory
// that nothing a brand-new process could be supervising survives a restart.
// That theory is exactly backwards for a live worker's own periodic push —
// calling register() again from the same still-running process would mark
// every job it is actually supervising right now as lost and quarantine the
// device out from under it, purely because a probe pass happened to finish.
// This route never touches UpsertWorker, or any worker/device/job state at
// all: only device_labels and host_docs.
func (s *Server) handlePushLabels(w http.ResponseWriter, r *http.Request) {
	var req LabelsPushRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Host == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "host required")
		return
	}

	// The {id} in the path must name a worker this controller actually
	// knows about, AND that worker's recorded host must match req.Host — a
	// fix-round-1 finding: without this, POSTing to an arbitrary or
	// typo'd worker ID silently wrote device_labels/host_docs rows for
	// whatever host the body claimed, with no verification the two agree.
	// A worker with a typo'd `host:` in worker.yaml would then push into
	// rows nothing ever reads (its real device IDs are "correct-host:name",
	// not "typo-host:name") while this route kept answering 200 forever —
	// a misconfigured node that looks perfectly healthy for months. Worse,
	// a typo that happened to COLLIDE with a different real host's name
	// would silently overwrite that host's labels instead.
	knownHost, err := s.cfg.Store.WorkerHost(r.PathValue("id"))
	if err != nil {
		if errors.Is(err, store.ErrWorkerNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "worker not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	if req.Host != knownHost {
		writeErr(w, http.StatusBadRequest, "bad_request", "host does not match the registered worker")
		return
	}

	if name := oversizedSheet(req.Sheet, req.DeviceSheets); name != "" {
		writeErr(w, http.StatusRequestEntityTooLarge, "sheet_too_large",
			fmt.Sprintf("usage sheet %q exceeds 64KB", name))
		return
	}

	now := s.cfg.Clock.Now()
	if err := s.applyDeviceFacts(req.Host, req.Devices, req.Labels, req.DeclaredLabels, req.Sheet, req.DeviceSheets, now); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	s.publishDevices()
	w.WriteHeader(http.StatusOK)
}

// oversizedSheet reports the name of the first sheet ("host", or a device's
// bare name) that exceeds maxSheetBytes, or "" if none do. A nil sheet (the
// worker has nothing trustworthy to say about the host sheet this round) is
// never oversized — there is nothing to check.
func oversizedSheet(sheet *string, deviceSheets map[string]string) string {
	if sheet != nil && len(*sheet) > maxSheetBytes {
		return "host"
	}
	for name, body := range deviceSheets {
		if len(body) > maxSheetBytes {
			return name
		}
	}
	return ""
}

// applyDeviceFacts stores what a worker reports about its devices — probed
// and declared labels, plus any host and per-device usage sheets — for
// every device named in deviceNames. It is shared, unchanged, by
// handleRegister and handlePushLabels: both must apply the exact same
// nil-vs-empty label contract and the exact same host-then-device merge, and
// a caller-specific duplicate of this logic would be exactly the kind of
// place the two could quietly drift.
//
// The nil-vs-empty contract is applied per DEVICE, not just once for the
// whole map: a nil labels (or declaredLabels) map skips that source for
// every device, but even a non-nil map only touches a device whose bare
// name is actually a KEY in it — see RegisterRequest.Labels and
// worker.labelsPayload for why a device can be legitimately absent from an
// otherwise non-nil map (its own probes failed this pass, so nothing about
// it is trustworthy) while a sibling device that DID report cleanly is
// still updated. A fix-round-1 review finding caught the earlier, coarser
// version of this: gatherLabels' built-ins (at minimum "cpus") make the
// TOP-LEVEL map non-nil on virtually every real pass, so a nil-check alone
// never actually protected a device whose OWN facts (e.g. nvidia-smi's
// GPU/VRAM keys) disappeared for one pass while the host-wide facts kept
// flowing.
//
// A present device key, even mapping to an empty submap, DOES replace via
// ReplaceLabels — that is the legitimate "this device's probes ran and
// found nothing" case Task 3's ReplaceLabels exists to support — merging
// that device's own host-wide value (the "" key) in first so a
// device-scoped value wins any key collision.
func (s *Server) applyDeviceFacts(host string, deviceNames []string,
	labels, declaredLabels map[string]map[string]string,
	sheet *string, deviceSheets map[string]string, now time.Time) error {

	if sheet != nil {
		if err := s.cfg.Store.UpsertHostDoc(host, "", *sheet, now); err != nil {
			return fmt.Errorf("store host sheet: %w", err)
		}
	}
	// else: the worker could not read host.md this round (see
	// RegisterRequest.Sheet) — leave whatever is already stored for this
	// host's sheet exactly as it was.

	hostLabels := labels[""]
	hostDeclared := declaredLabels[""]

	for _, name := range deviceNames {
		deviceID := host + ":" + name

		if labels != nil {
			if deviceLabels, ok := labels[name]; ok {
				if err := s.cfg.Store.ReplaceLabels(deviceID, model.SourceDetected,
					mergeLabelMaps(hostLabels, deviceLabels), now); err != nil {
					return fmt.Errorf("replace detected labels for %s: %w", deviceID, err)
				}
			}
			// else: this device has no key in labels at all — the worker
			// has nothing trustworthy to say about it this round, so its
			// previously stored detected labels are left exactly as they
			// were.
		}
		if declaredLabels != nil {
			if deviceDeclared, ok := declaredLabels[name]; ok {
				if err := s.cfg.Store.ReplaceLabels(deviceID, model.SourceDeclared,
					mergeLabelMaps(hostDeclared, deviceDeclared), now); err != nil {
					return fmt.Errorf("replace declared labels for %s: %w", deviceID, err)
				}
			}
		}
		if body, ok := deviceSheets[name]; ok {
			if err := s.cfg.Store.UpsertHostDoc(host, deviceID, body, now); err != nil {
				return fmt.Errorf("store sheet for %s: %w", deviceID, err)
			}
		}
	}
	return nil
}

// mergeLabelMaps combines host-wide facts with one device's own, letting the
// device-scoped value win any key collision — a device fact is more
// specific than a host-wide one, so it must never be shadowed by it. It
// always returns a fresh, non-nil map, even when both inputs are empty, so
// a caller can pass it straight to ReplaceLabels without a nil check.
func mergeLabelMaps(host, device map[string]string) map[string]string {
	out := make(map[string]string, len(host)+len(device))
	for k, v := range host {
		out[k] = v
	}
	for k, v := range device {
		out[k] = v
	}
	return out
}

// handleHeartbeat renews the leases of the jobs the worker names, and only
// those. See store.RecordHeartbeat for why: a worker being alive says nothing
// about whether it is running any particular job, and renewing on liveness
// alone made lease expiry unreachable for a job whose assignment response was
// lost.
//
// A body is optional so a heartbeat from a worker with nothing running can
// stay a bare POST. An absent or empty body means "I am running nothing",
// which is the honest reading: a worker that cannot name a job is not
// evidence that the job is alive.
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHeartbeatBody+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if len(body) > maxHeartbeatBody {
		writeErr(w, http.StatusRequestEntityTooLarge, "body_too_large", "heartbeat body exceeds 64KB")
		return
	}
	var req HeartbeatRequest
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
	}

	if err := s.cfg.Store.RecordHeartbeat(
		r.PathValue("id"), s.cfg.Clock.Now(), req.RunningJobIDs); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAssignments long-polls: it returns as soon as this worker has work,
// otherwise 204 after the wait window so the worker simply calls again.
func (s *Server) handleAssignments(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")

	wait := 30 * time.Second
	if v := r.URL.Query().Get("wait"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 && d <= time.Minute {
			wait = d
		}
	}

	woken, cancel := s.notify.wait(workerID)
	defer cancel()

	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	for {
		jobs, err := s.cfg.Store.AssignedJobsFor(workerID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		// A kill must reach the worker as fast as an assignment does, so it
		// rides the same long-poll response rather than a separate channel:
		// checked on every wake, and on its own enough to end the wait.
		//
		// This is a take: each flag handed out here is stamped delivered and
		// stays quiet for killRedeliverInterval. Ending the poll early is
		// exactly what a kill is for, but a flag on a job no worker is
		// actually running would otherwise end EVERY poll instantly and the
		// worker would re-poll at its floor interval forever. Spacing
		// re-delivery keeps the fast path fast and bounds the pathological
		// one.
		kills, err := s.cfg.Store.TakeKillRequests(workerID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		if len(jobs) > 0 || len(kills) > 0 {
			// Handing out an assignment is a state transition, not just a
			// query: mark each job running immediately, before it is ever
			// written to the response. Otherwise the job stays "assigned"
			// until the worker's own "running" report happens to land, and a
			// second poll landing in that window (or a retried poll after a
			// dropped response) sees it as "assigned" again and hands it out
			// — and therefore starts it — a second time.
			//
			// This makes started_at mark handout rather than the moment the
			// worker actually spawns the process, which is a deliberate,
			// documented shift: if the worker dies between handout and spawn,
			// the job simply sits in "running" until the reaper marks it
			// lost, which is the correct outcome since nobody actually knows
			// whether it started. The worker's own "running" report becomes a
			// harmless no-op — MarkRunning only transitions assigned->running,
			// so calling it again once already running affects no rows.
			now := s.cfg.Clock.Now()
			out := make([]Assignment, 0, len(jobs))
			for _, j := range jobs {
				if err := s.cfg.Store.MarkRunning(j.ID, now); err != nil {
					writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
					return
				}
				out = append(out, Assignment{
					JobID: j.ID, DeviceID: j.DeviceID, Command: j.Command, Cwd: j.Cwd, Env: j.Env,
					MaxRuntimeSeconds: j.MaxRuntimeSeconds, IdleTimeoutSeconds: j.IdleTimeoutSeconds,
					Submitter: j.Submitter, Kind: j.Kind,
				})
			}
			writeJSON(w, http.StatusOK, PollResponse{Assignments: out, Kills: kills})
			return
		}
		select {
		case <-woken:
		case <-deadline.C:
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
}

// handleDeviceFault is how a worker takes one of its own devices out of the
// pool. It has two producers, not one: a worker whose on_acquire hook failed
// calls it instead of ever starting the job, and a worker whose post-job
// verify pass failed calls it before reporting that job terminal — so the
// device is already quarantined when Release runs and cannot be handed to
// the next assignment. (The prefix on Reason is what tells the two apart
// here; see verifyReasonPrefix below.)
// SetDeviceState already records DeviceUnhealthy set through this path with
// quarantine reason "fault" — the one cause rebootClearableReasons
// (internal/store/reaper.go) deliberately excludes, since a reboot proves no
// process survived but nothing about the hardware (or, here, the service
// the hook could not stop). Only an admin's explicit
// POST /v1/devices/{id}/clear puts it back.
func (s *Server) handleDeviceFault(w http.ResponseWriter, r *http.Request) {
	var req FaultRequest
	if !decode(w, r, &req) {
		return
	}
	id := r.PathValue("id")
	if req.Reason == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "reason required")
		return
	}
	if err := s.cfg.Store.SetDeviceState(id, model.DeviceUnhealthy, s.cfg.Clock.Now(), req.Reason); err != nil {
		// A device ID this controller has never heard of must not answer 200:
		// the worker would log a successful quarantine having changed nothing,
		// and the device (whatever its real ID is) stays schedulable while
		// everyone believes it was taken out of the pool. Say plainly that
		// nothing matched.
		if errors.Is(err, store.ErrDeviceNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "device not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	slog.Warn("device quarantined: fault", "device", id, "reason", req.Reason)
	s.publishDevices()

	// A verify failure IS a device going unhealthy, so emitting both kinds
	// would make any consumer counting device_unhealthy double count. The
	// specific kind wins; the general one is for every other source.
	kind := notify.KindDeviceUnhealthy
	if strings.HasPrefix(req.Reason, verifyReasonPrefix) {
		kind = notify.KindVerifyFailed
	}
	s.emit(notify.Event{Kind: kind, Device: id, Job: s.activeJobOn(id), Reason: req.Reason})
	w.WriteHeader(http.StatusOK)
}

// verifyReasonPrefix is the literal prefix internal/worker/verify.go stamps
// on the reason of every verify-sourced fault. The fault endpoint takes free
// text — a failed lifecycle hook posts its own tail output through the same
// route — so this prefix is the only thing distinguishing the two sources.
// internal/worker pins the exact string in a test of its own, so it cannot
// drift silently; changing it there without changing it here would demote
// every verify failure to a plain device_unhealthy.
const verifyReasonPrefix = "verify failed: "

// watchdogReasons are the reasons internal/worker/exec.go writes when one of
// its two watchdogs trips, matched as prefixes because each is completed
// with the configured limit ("max_runtime exceeded (4h0m0s)").
//
// Matching the reason rather than the `killed` state is the whole point: a
// job killed by `rc kill`, or by a worker shutting down, is reported killed
// too and is nobody's incident. exec.go labels those "cancelled".
var watchdogReasons = []string{"max_runtime exceeded", "idle: no output for"}

func isWatchdogReason(reason string) bool {
	for _, prefix := range watchdogReasons {
		if strings.HasPrefix(reason, prefix) {
			return true
		}
	}
	return false
}

// activeJobOn names the job currently holding deviceID, or "" if none is —
// what a verify failure needs to say WHICH run left the device dirty. The
// worker verifies before it reports the job terminal (see internal/worker),
// so at fault time that job is still assigned or running and this finds it.
//
// It never fails the request it serves: a fault must be recorded and
// announced whether or not the job behind it can be named, so a store error
// here degrades the event to one without a job rather than rejecting a
// quarantine the store already applied.
func (s *Server) activeJobOn(deviceID string) string {
	id, err := s.cfg.Store.ActiveJobOnDevice(deviceID)
	if err != nil {
		slog.Warn("could not resolve the job on a faulted device", "device", deviceID, "err", err)
		return ""
	}
	return id
}

func (s *Server) handleAppendLogs(w http.ResponseWriter, r *http.Request) {
	chunk, err := io.ReadAll(io.LimitReader(r.Body, maxLogChunk+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if len(chunk) > maxLogChunk {
		writeErr(w, http.StatusRequestEntityTooLarge, "chunk_too_large", "log chunk exceeds 1MB; split and retry")
		return
	}
	if err := s.cfg.Logs.Append(r.PathValue("id"), chunk); err != nil {
		writeErr(w, http.StatusInternalServerError, "log_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	var req StatusRequest
	if !decode(w, r, &req) {
		return
	}
	jobID := r.PathValue("id")

	job, err := s.cfg.Store.Job(jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "not_found", "job not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	// A job belongs to the worker it was assigned to. Without this check any
	// holder of a worker token could report status for someone else's job and
	// have Release() hand its device to a second job while the first is still
	// running on it — exactly the race this system exists to prevent.
	if req.WorkerID == "" || req.WorkerID != job.WorkerID {
		writeErr(w, http.StatusForbidden, "not_job_owner", "job is not assigned to this worker")
		return
	}

	if req.State == model.JobRunning {
		if err := s.cfg.Store.MarkRunning(jobID, s.cfg.Clock.Now()); err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		s.publishJob(jobID, req.State)
		w.WriteHeader(http.StatusOK)
		return
	}
	if !req.State.Terminal() {
		writeErr(w, http.StatusBadRequest, "bad_request", "state must be running or terminal")
		return
	}
	if err := s.cfg.Store.Release(jobID, req.State, req.ExitCode, req.Reason); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	s.publishJob(jobID, req.State)
	// Only a terminal report can be a watchdog trip, and only one whose
	// reason names a watchdog — see isWatchdogReason for why the `killed`
	// state is not the signal.
	//
	// job.State is the state BEFORE the Release above, which is what makes
	// this idempotent. Release deliberately treats an already-terminal job
	// as a silent success (see store.Release), and the ownership check
	// above still passes afterwards, so a worker retrying a terminal report
	// whose response was lost — reportTerminalWithRetry tries five times —
	// would otherwise page an operator once per attempt for one runaway job.
	//
	// A hold is excluded outright. Its --ttl becomes MaxRuntimeSeconds, so
	// the worker's sleeper is killed by the wall-clock watchdog on every
	// single hold that runs to its end: that is the hold expiring exactly as
	// asked, in the same category as `rc kill`, not an incident. It would
	// also be the highest-volume event in the whole set, which is the
	// fastest way to teach an operator to ignore the webhook.
	if !job.State.Terminal() && job.Kind != model.LeaseKindHold && isWatchdogReason(req.Reason) {
		s.emit(notify.Event{
			Kind: notify.KindWatchdogTrip, Job: jobID, Device: job.DeviceID, Reason: req.Reason,
		})
	}
	w.WriteHeader(http.StatusOK)
}
