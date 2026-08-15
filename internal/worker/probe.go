package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ProbeResult is what one gatherLabels pass produced: host-wide facts that
// apply to every device on this host, facts scoped to one device by name,
// and the devices whose own facts this pass could not confirm. All three
// maps are always non-nil, even when nothing was gathered, so a caller never
// has to nil-check before ranging or indexing.
type ProbeResult struct {
	Host   map[string]string
	Device map[string]map[string]string

	// Unconfirmed names the devices whose OWN device-scoped facts this pass
	// could not confirm, because a source understood to speak for that
	// device failed: nvidia-smi found on PATH but erroring, timing out, or
	// emitting something that doesn't parse; nvidia-smi not found on PATH
	// THIS pass despite having been found on this worker process's FIRST
	// pass (that one snapshot, w.nvidiaSmiSeenAtStartup, is the whole
	// history kept — see gatherLabels for the "seen at startup" rule this
	// depends on; a host that has never once seen nvidia-smi does NOT mark
	// anything unconfirmed for that reason, or a device's labels could never
	// be cleared on a GPU-less host); or a drop-in probe erroring, being
	// skipped for budget, or failing to stat.
	//
	// This is what lets a caller (see labelsPayload) tell "this device's
	// probes ran and confirmed there is nothing to report" — which
	// ReplaceLabels must be allowed to turn into a clear, per Task 3's own
	// design — apart from "something that can produce THIS device's facts
	// broke this pass, so an empty result here proves nothing about the
	// device itself".
	//
	// It is per DEVICE, not one flag for the whole pass. The pass-wide
	// `Failed` bool this replaces argued that a single flag was NECESSARY,
	// because gatherLabels "has no way to know in advance which device a
	// probe would have named had it succeeded". That premise is right, and
	// it is still why the attribution below is deliberately conservative —
	// but the conclusion drawn from it was wrong, and Stage 3's final
	// review measured the damage:
	//
	//   - HOST-scoped facts apply to every device by construction, so one
	//     source failing cannot make ANOTHER source's host facts doubtful.
	//     Under the pass-wide flag it did exactly that: with one unrelated
	//     drop-in broken, disk_free_bytes and mem_total_bytes froze at
	//     their last good values on every device of the host — facts
	//     builtinLabels had gathered perfectly well on that same pass —
	//     because a device omitted from the payload never gets
	//     ReplaceLabels called on it at all (see labelsPayload here, and
	//     applyDeviceFacts server-side). A full box kept advertising
	//     disk_free_bytes=72G, `--select 'disk_free_bytes>=50G'` routed a
	//     job onto it, and the job died on write.
	//   - Which devices a source can speak for is not, in fact, wholly
	//     unknowable. nvidia-smi's keys are "gpu<N>.<label>" by
	//     construction, so it can only ever name a device this host
	//     declares under that exact form — see nvidiaScopedDevices. A
	//     drop-in script is opaque, but its LAST SUCCESSFUL run in THIS
	//     process is evidence about it: a script that named gpu0 the last
	//     time it worked is understood to speak for gpu0, and for nothing
	//     else, until it succeeds again and says otherwise — see
	//     Worker.probeSourceDevices.
	//
	// What has NOT changed is what being unconfirmed then means: preserve,
	// do not clear. A device named here is omitted from the payload
	// entirely rather than sent as an empty map, exactly as before, because
	// wiping a device's facts on a guess is fleet-wide while a stale label
	// is one device.
	//
	// A source this worker process has never once seen succeed speaks for
	// no device, so its failure marks nothing unconfirmed. That is the
	// deliberate cost of the fix: a drop-in that reported gpu0's facts for
	// a PREVIOUS process and is broken by the time this one starts will
	// have those facts cleared on the first pass, instead of frozen
	// forever. It is the same trade gatherLabels already makes for
	// nvidia-smi's "seen at startup" rule (see its comment there, and the
	// residual it accepts): a process restart is a far narrower window than
	// every probe interval on every host for the rest of a worker's life,
	// and registration is already the moment the controller reconciles a
	// worker's whole world. nvidia-smi itself is exempt — its scope is
	// static, so it needs no such history (nvidiaScopedDevices again).
	Unconfirmed map[string]bool
}

// probePassBudget bounds the WHOLE gatherLabels pass — every built-in call
// and every drop-in probe together, not each one's own per-probe timeout
// summed. Without this, N probes each capped at ProbeTimeout still
// serialise to N*ProbeTimeout: 20 drop-ins at the 5s default is 100s,
// comfortably past the worker's heartbeat interval (default 10s). Whatever
// the budget cuts short is simply skipped for this pass — gatherLabels runs
// again on ProbeInterval, so a probe that lost its turn gets another one
// soon, and nothing here is "lost" the way a startup hook's crash-recovery
// pass would be.
//
// A var, not a const, so a test can shrink it without waiting out a real
// 30s budget.
var probePassBudget = 30 * time.Second

// gatherLabels discovers this host's device facts: the built-ins (CPU count,
// memory, disk, kernel, and — when nvidia-smi is on PATH — GPU facts), then
// every executable in cfg.ProbeDir, run in name order.
//
// The governing rule is that a probe which fails, times out, or emits
// something that isn't a flat JSON object is logged and skipped; whatever
// succeeded is kept. This pass must never block Start or registration on one
// misbehaving diagnostic script — a wedged nvidia-smi costs a label, never a
// worker.
//
// Later probes overwrite earlier keys for the same fact: 20-x.sh wins over
// 10-x.sh, and drop-in probes win over the built-ins that ran before them.
//
// Failure is tracked PER SOURCE, and attributed to the devices that source
// is understood to speak for — see ProbeResult.Unconfirmed for the full
// reasoning, and probeSourceDevices for how a drop-in's scope is learned.
// A source that failed never makes another source's facts doubtful: a host
// fact gathered successfully this pass reaches every device even when some
// other probe on the box is broken.
func (w *Worker) gatherLabels(ctx context.Context) ProbeResult {
	ctx, cancel := context.WithTimeout(ctx, probePassBudget)
	defer cancel()

	deviceNames := make(map[string]struct{}, len(w.cfg.Devices))
	res := ProbeResult{
		Host:        map[string]string{},
		Device:      map[string]map[string]string{},
		Unconfirmed: map[string]bool{},
	}
	for _, d := range w.cfg.Devices {
		deviceNames[d.Name] = struct{}{}
		res.Device[d.Name] = map[string]string{}
	}

	// sourceDevices is this pass's rebuild of w.probeSourceDevices, swapped
	// in when the pass ends (including via an early return). Rebuilding
	// rather than mutating in place is what retires the memory of a source
	// that is simply GONE — a drop-in deleted, renamed, or chmod -x'd — so
	// the map cannot grow without bound over a worker's lifetime, and so a
	// removed probe's facts clear the ordinary way (it did not fail; it is
	// not configured any more) instead of being preserved forever by a
	// memory nothing ever cleans up. A source that ran and FAILED carries
	// its entry forward explicitly, via unconfirm.
	sourceDevices := make(map[string]map[string]bool, len(w.probeSourceDevices))
	defer func() {
		w.probeSourceDevices = sourceDevices
		// probeSourceClearWarned is pruned against the same rebuild, so a
		// source that is gone cannot leave a marker behind that silences the
		// warning if a script of that name ever comes back.
		for source := range w.probeSourceClearWarned {
			if _, known := sourceDevices[source]; !known {
				delete(w.probeSourceClearWarned, source)
			}
		}
	}()

	// merge is called only for a source that actually SUCCEEDED: it folds
	// that source's facts in and records which devices it named, replacing
	// whatever it named last time — a probe that stops reporting gpu0 stops
	// speaking for gpu0.
	merge := func(source string, facts map[string]string) {
		sourceDevices[source] = mergeProbeFacts(&res, facts, deviceNames, source)
		delete(w.probeSourceClearWarned, source) // recovered: a later failure warns again
	}

	// unconfirm is the failure half: this source could not be confirmed
	// this pass, so every device it is understood to speak for is
	// unconfirmed, and its memory of them is carried forward unchanged
	// (nothing this pass learned anything about it). Carrying it forward is
	// what makes preservation last as long as the outage does: without it a
	// device would be preserved on the first failing pass and then, its
	// scope forgotten, sent as a present empty map on the second — the
	// clear this whole mechanism exists to prevent, arriving one probe
	// interval late.
	unconfirm := func(source string, devices map[string]bool) {
		sourceDevices[source] = devices
		for name := range devices {
			res.Unconfirmed[name] = true
		}
	}

	// dropInFailed is unconfirm for a drop-in probe, whose scope — unlike
	// nvidia-smi's — is known only from what it named the last time it
	// worked. When there is no such record, this source covers no device and
	// nothing is preserved for it: whatever it used to report is about to be
	// cleared. That is the deliberate ruling (see ProbeResult.Unconfirmed),
	// but it must not be SILENT. Without this line an operator sees "probe
	// failed; skipped" on the worker and, entirely separately, labels
	// vanishing and submits being rejected, with nothing connecting the two
	// but a hunch — and no notify event covers labels. Warned once per
	// episode, per source, in the same shape labelOmitWarned uses for the
	// omission warning: an outage lasts many passes and a line per pass
	// teaches an operator to stop reading the stream.
	dropInFailed := func(path string) {
		devices := w.probeSourceDevices[path]
		unconfirm(path, devices)
		if len(devices) > 0 || w.probeSourceClearWarned[path] {
			return
		}
		slog.Warn("probe failed and has not succeeded once since this worker started; "+
			"it is treated as covering no device, so anything it used to report is cleared rather than preserved",
			"probe", path, "devices", declaredDeviceNames(w.cfg.Devices))
		w.probeSourceClearWarned[path] = true
	}

	merge("builtin", builtinLabels())

	nvFacts, nvFound, nvBroken := nvidiaLabels(ctx, w.cfg.ProbeTimeout)
	if nvFound && !nvBroken {
		merge(nvidiaSource, nvFacts)
	}

	// nvidiaSmiSeenAtStartup is recorded exactly once, on this worker's
	// very first probe pass (always inside register(), before probeLoop's
	// goroutine is even started — see Start — so there is no concurrent
	// access to worry about, the same happens-before guarantee w.workerID
	// already relies on elsewhere in this package). Every later pass only
	// READS it, never updates it. This is the fix-round-3 ruling: fix
	// round 2 made ANY absence of nvidia-smi a failure, which correctly
	// closed the fleet-wide-wipe scenario (a driver upgrade removing the
	// binary) but, measured on a host that has NEVER had nvidia-smi at
	// all, meant the pass reported a failure on literally every pass
	// forever — which in turn meant a device's detected labels could never be
	// cleared by ANY means on such a host, not even the ordinary "a
	// drop-in probe stopped reporting a key" case. Distinguishing "never
	// present since this worker started" (not a failure — this host
	// simply has no NVIDIA tooling, labels clear normally) from "present
	// earlier, now gone" (a failure — the exact upgrade/reinstall window
	// the round-2 ruling exists for) fixes that without reopening the
	// fleet-wide wipe.
	//
	// The accepted residual: a worker RESTARTED during an upgrade window
	// that removed the binary sees "never present" on its own first pass
	// and will clear that device's labels. This is deliberately not
	// treated as a bug — registration is already the moment the
	// controller reconciles a worker's whole world (see UpsertWorker,
	// store side), and a process restart is a far narrower window than
	// every probe interval on every host for the rest of that worker's
	// life.
	if !w.nvidiaSmiStartupChecked {
		w.nvidiaSmiStartupChecked = true
		w.nvidiaSmiSeenAtStartup = nvFound
	}

	// nvidia-smi's scope needs no history: its keys are "gpu<N>.<label>" by
	// construction, so the devices it could ever speak for are exactly the
	// ones this host declares under that form. Once it HAS succeeded here,
	// the devices it actually named are narrower still (a box declaring
	// gpu0..gpu7 with two cards installed), so prefer that.
	if nvBroken || (!nvFound && w.nvidiaSmiSeenAtStartup) {
		scope := w.probeSourceDevices[nvidiaSource]
		if len(scope) == 0 {
			scope = nvidiaScopedDevices(deviceNames)
		}
		unconfirm(nvidiaSource, scope)
	}

	entries, err := os.ReadDir(w.cfg.ProbeDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No probe directory is a legitimate, confirmed configuration —
			// this host simply has no drop-in probes — so nothing is
			// unconfirmed and no drop-in memory survives it.
			return res
		}
		// Anything else (permission denied, not-a-directory, ...) is worth a
		// line, but still not fatal: the built-ins above already ran. Not one
		// drop-in got the chance to run, though, so every device any of them
		// speaks for is unconfirmed — a `chmod 000` on probe.d must not clear
		// the whole fleet's detected labels the way a directory that ran and
		// legitimately reported nothing would.
		slog.Warn("read probe dir", "dir", w.cfg.ProbeDir, "err", err)
		for source := range w.probeSourceDevices {
			if _, ran := sourceDevices[source]; ran {
				// A built-in, already resolved above: nvidia-smi that ran
				// perfectly well this pass must not be re-clouded by a
				// failure that belongs to the drop-in directory — that is
				// this task's own bug class, one branch over.
				continue
			}
			dropInFailed(source)
		}
		return res
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // execution order is name order, so "20-x" overwrites "10-x"

	for i, name := range names {
		if err := ctx.Err(); err != nil {
			slog.Warn("probe pass exceeded its budget; remaining probes skipped",
				"dir", w.cfg.ProbeDir, "budget", probePassBudget)
			// A probe skipped for budget reasons never ran at all, so
			// whatever it would have said about the devices it speaks for is
			// simply unknown — indistinguishable, for labelsPayload's
			// purposes, from one that ran and failed outright. That goes for
			// every probe still ahead of this one too, not just this one.
			for _, skipped := range names[i:] {
				dropInFailed(filepath.Join(w.cfg.ProbeDir, skipped))
			}
			break
		}

		path := filepath.Join(w.cfg.ProbeDir, name)
		info, err := os.Stat(path)
		if err != nil {
			slog.Warn("stat probe; skipped", "probe", path, "err", err)
			dropInFailed(path)
			continue
		}
		if info.Mode()&0o111 == 0 {
			continue // not executable: e.g. a README dropped alongside probes
		}

		facts, err := runProbe(ctx, path, w.cfg.ProbeTimeout)
		if err != nil {
			slog.Warn("probe failed; skipped", "probe", path, "err", err)
			dropInFailed(path)
			continue
		}
		merge(path, facts)
	}
	return res
}

// declaredDeviceNames lists this host's device names, sorted, for a log line
// that has to say which devices a decision affects.
func declaredDeviceNames(devices []DeviceConfig) []string {
	names := make([]string, 0, len(devices))
	for _, d := range devices {
		names = append(names, d.Name)
	}
	sort.Strings(names)
	return names
}

// nvidiaSource names nvidia-smi as a probe source, both in the warning
// mergeProbeFacts logs and as the key its learned device scope is
// remembered under.
const nvidiaSource = "nvidia-smi"

// nvidiaDeviceKeyPrefix is the fixed prefix nvidiaLabels names its
// device-scoped keys with, followed by nvidia-smi's own device index:
// "gpu0.vram", "gpu1.model", ... It lives here, shared with
// nvidiaScopedDevices, so the two cannot drift: the set of devices whose
// labels are preserved when nvidia-smi breaks is derived from the exact same
// naming rule that decides which devices it can label in the first place.
const nvidiaDeviceKeyPrefix = "gpu"

// nvidiaScopedDevices returns every declared device nvidia-smi's facts could
// ever land on: those named exactly nvidiaDeviceKeyPrefix followed by a
// non-empty run of digits, since classifyKey only accepts a dotted key whose
// prefix matches a declared device name verbatim. On a host whose devices
// are named "a100-0"/"rtx-0", nvidia-smi breaking says nothing whatsoever
// about them — it could not have labelled them even when it worked — so
// they keep refreshing normally instead of freezing behind a failure that
// was never theirs.
func nvidiaScopedDevices(deviceNames map[string]struct{}) map[string]bool {
	scope := map[string]bool{}
	for name := range deviceNames {
		digits, ok := strings.CutPrefix(name, nvidiaDeviceKeyPrefix)
		if !ok || digits == "" {
			continue
		}
		allDigits := true
		for _, r := range digits {
			if r < '0' || r > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			scope[name] = true
		}
	}
	return scope
}

// keyKind is what mergeProbeFacts decided one probe-emitted key is.
type keyKind int

const (
	keyHost keyKind = iota
	keyDevice
	keyInvalid
)

// classifyKey decides where one flat-JSON key belongs. A key with no dot is
// a host fact, applied to every device. A key with a dot names a device it
// targets — the substring before the first dot — so it is a fact for that
// device ONLY if this host actually declares a device by that name;
// otherwise it is dropped, not promoted to a host fact. Promoting an
// unrecognised device-scoped key to Host used to mean a probe emitting
// "gpu0.model"/"gpu1.model" on a host whose devices are actually named
// "a100-0"/"rtx-0" silently applied BOTH GPUs' models to EVERY device —
// exactly the kind of wrong-card scheduling this whole mechanism exists to
// prevent. A malformed dotted key (empty prefix or suffix — ".lead",
// "trail.") or an empty key ("") is dropped the same way: it was never a
// valid host key or a valid device key.
func classifyKey(key string, deviceNames map[string]struct{}) (kind keyKind, dev, label string) {
	if key == "" {
		return keyInvalid, "", ""
	}
	i := strings.Index(key, ".")
	if i < 0 {
		return keyHost, "", key
	}
	if i == 0 || i == len(key)-1 {
		return keyInvalid, "", ""
	}
	prefix := key[:i]
	if _, known := deviceNames[prefix]; !known {
		return keyInvalid, "", ""
	}
	return keyDevice, prefix, key[i+1:]
}

// mergeProbeFacts applies one probe's flat fact map into res, classifying
// every key via classifyKey. source names where these facts came from
// ("builtin", "nvidia-smi", or a probe's path) purely for the warning
// logged when a key is dropped.
//
// It returns the devices this source actually named — the evidence
// gatherLabels remembers about which devices this source speaks for, so a
// later failure of the SAME source is attributed to those devices and to no
// others. A source that named none returns an empty (never nil) map: it
// speaks for no device, and its failure freezes nothing.
func mergeProbeFacts(res *ProbeResult, facts map[string]string, deviceNames map[string]struct{}, source string) map[string]bool {
	named := map[string]bool{}
	for k, v := range facts {
		switch kind, dev, label := classifyKey(k, deviceNames); kind {
		case keyHost:
			res.Host[k] = v
		case keyDevice:
			if res.Device[dev] == nil {
				res.Device[dev] = map[string]string{}
			}
			res.Device[dev][label] = v
			named[dev] = true
		default: // keyInvalid
			slog.Warn("probe key names no known device or is malformed; dropped", "source", source, "key", k)
		}
	}
	return named
}

// maxProbeOutputBytes bounds how much of a probe's stdout (and, separately,
// its stderr) is retained before that stream is judged pathological. A
// label set is a handful of short strings; a probe still producing output
// past this is misbehaving, not merely verbose. Measured: a probe emitting
// 200MB was fully buffered in 1.57s — well inside any sane timeout, so a
// slow timeout alone does not bound memory here; only a hard cap on what's
// kept does.
const maxProbeOutputBytes = 64 * 1024

// boundedBuffer accepts every byte written to it — so the writer (a pipe
// copier inside os/exec) never blocks or errors on a full buffer, and a
// chatty child always gets to finish or hit its own MaxRuntime — but keeps
// at most limit bytes of it. Anything past that is read and discarded, and
// truncated is set so the caller can tell "this stream was capped" apart
// from "this stream just happened to be short".
type boundedBuffer struct {
	limit     int
	buf       bytes.Buffer
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if !b.truncated {
		room := b.limit - b.buf.Len()
		if room > len(p) {
			room = len(p)
		}
		if room > 0 {
			b.buf.Write(p[:room])
		}
		if room < len(p) {
			b.truncated = true
		}
	}
	return len(p), nil
}

// stderrNote renders a short, single-line summary of a probe's stderr for
// inclusion in a warning log line — enough to show why a probe failed
// without shipping unbounded text into a log message.
func stderrNote(stderr *boundedBuffer) string {
	s := strings.TrimSpace(stderr.buf.String())
	if s == "" {
		return ""
	}
	if len(s) > 200 {
		s = s[:200] + "...(truncated)"
	}
	if stderr.truncated {
		s += " (stderr itself was truncated)"
	}
	return "; stderr: " + s
}

// runProbe executes one probe under the same process-group supervision a job
// or a lifecycle hook gets (Run, in exec.go): its own process group, killed
// whole on timeout, so a wedged probe never leaks a process nor stalls the
// pass past its own budget. Stdout and stderr are captured SEPARATELY (via
// JobSpec.Stderr) specifically so a probe that writes a warning to stderr
// and still exits 0 with valid JSON on stdout does not lose every label it
// produced — merging the two into one buffer, as jobs and hooks
// deliberately do, would make stderr output indistinguishable from
// corruption of the JSON this function has to parse.
//
// A non-zero exit, a kill (timeout or otherwise), oversized output, or
// output that doesn't parse as a single flat JSON object is reported as an
// error; the caller logs it and moves on to the next probe.
func runProbe(ctx context.Context, path string, timeout time.Duration) (map[string]string, error) {
	stdout := &boundedBuffer{limit: maxProbeOutputBytes}
	stderr := &boundedBuffer{limit: maxProbeOutputBytes}
	res := Run(ctx, JobSpec{Command: []string{path}, MaxRuntime: timeout, Stderr: stderr}, stdout)

	if res.Err != nil {
		return nil, fmt.Errorf("run: %w", res.Err)
	}
	if res.Killed {
		reason := res.Reason
		if reason == "" {
			reason = "killed"
		}
		return nil, fmt.Errorf("killed: %s%s", reason, stderrNote(stderr))
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("exited %d%s", res.ExitCode, stderrNote(stderr))
	}
	if stdout.truncated {
		return nil, fmt.Errorf("stdout exceeded %d bytes; probe misbehaving", maxProbeOutputBytes)
	}
	return parseProbeOutput(path, stdout.buf.Bytes())
}

// parseProbeOutput turns one probe's stdout into a flat string map. Values
// are stringified: a JSON number keeps its own decimal text (via
// json.Number, not a float round-trip, so no precision is invented or
// lost), and a bool becomes "true"/"false". A nested object or array is not
// a scalar fact, so its key is dropped with a warning rather than failing
// the whole probe. Output that isn't a single JSON object — garbage, an
// array, a bare scalar, or a second value trailing the first — fails the
// whole probe: there is no way to salvage a partial fact set from something
// that isn't the documented shape.
func parseProbeOutput(path string, data []byte) (map[string]string, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse output: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("parse output: trailing data after the JSON object")
	}

	facts := make(map[string]string, len(raw))
	for k, v := range raw {
		switch t := v.(type) {
		case string:
			facts[k] = t
		case json.Number:
			facts[k] = t.String()
		case bool:
			facts[k] = strconv.FormatBool(t)
		case nil:
			// null carries no fact worth keeping; drop it silently.
		default:
			// object or array: not a scalar, so not a label.
			slog.Warn("probe emitted a non-scalar value; key skipped", "probe", path, "key", k)
		}
	}
	return facts, nil
}

// builtinLabels gathers host facts Go can determine on its own, without
// shelling out: CPU count, total RAM, free disk, and kernel version. Any
// individual fact that can't be read (a non-Linux /proc layout, a statfs
// failure) is simply omitted — a missing built-in is exactly as harmless as
// a missing drop-in probe result, never a reason to fail the pass.
func builtinLabels() map[string]string {
	facts := map[string]string{
		"cpus": strconv.Itoa(runtime.NumCPU()),
	}

	if kb, ok := memTotalKB(); ok {
		facts["mem_total_bytes"] = strconv.FormatUint(kb*1024, 10)
	}

	// The worker has no configured data directory of its own (unlike the
	// controller's --data flag) to statfs, so this reports free space on
	// root — a reasonable stand-in for "how much disk this host has left",
	// which is all a scheduling label needs to be.
	if free, ok := diskFreeBytes("/"); ok {
		facts["disk_free_bytes"] = strconv.FormatUint(free, 10)
	}

	if kernel, ok := kernelRelease(); ok {
		facts["kernel"] = kernel
	}

	return facts
}

// memTotalKB reads MemTotal, in kilobytes, out of /proc/meminfo.
func memTotalKB() (uint64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line) // "MemTotal:", "32871160", "kB"
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb, true
	}
	return 0, false
}

// diskFreeBytes reports free space (as available to an unprivileged caller)
// on the filesystem containing path.
func diskFreeBytes(path string) (uint64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, false
	}
	return stat.Bavail * uint64(stat.Bsize), true
}

// kernelRelease reads the running kernel's release string, e.g.
// "6.8.0-136-generic", from /proc/sys/kernel/osrelease.
func kernelRelease() (string, bool) {
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return "", false
	}
	release := strings.TrimSpace(string(b))
	if release == "" {
		return "", false
	}
	return release, true
}

// nvidiaLabels reports GPU facts from nvidia-smi. It reports two independent
// things about THIS pass, deliberately kept separate rather than folded
// into one "failed" bool (fix round 3 — see below for why):
//
//   - found: whether nvidia-smi was located on PATH at all this pass
//     (exec.LookPath succeeding), regardless of whether running it then
//     worked.
//   - broken: true only when nvidia-smi WAS found but its invocation or
//     output was no good (a non-zero exit, a timeout, output that doesn't
//     parse) — the "driver upgrade broke nvidia-smi" case, where the
//     binary stays on PATH and just stops working: "Failed to initialize
//     NVML: Driver/library version mismatch" is the canonical example.
//     broken is always false when found is false.
//
// Fix round 2 folded "not found" and "broken" into one failed bool, both
// true, so a device's stale GPU facts would survive either. Fix round 3
// measured the fallout on a host that has NEVER had nvidia-smi at all: with
// found permanently false, gatherLabels' caller had no way to tell that
// apart from "nvidia-smi vanished after being seen", so every pass reported
// a failure forever (the pass-wide flag that preceded
// ProbeResult.Unconfirmed), and a device's detected labels could
// never be cleared by ANY means on such a host — not even the ordinary
// "a drop-in probe stopped reporting a key" case Task 3 explicitly
// supports. gatherLabels (its only caller) now resolves the distinction
// itself using its own worker-lifetime state — see the fix-round-3 comment
// there for the "recorded once at startup" rule that keeps a genuinely
// GPU-less host able to clear labels normally while still preserving them
// on the fleet-wide-wipe scenario fix round 2 exists for.
//
// nvidia-smi is run through the SAME Run() process-group supervision every
// other spawned process in this package gets: its own process group,
// bounded by timeout, killed as a group (not just its immediate pid) if it
// doesn't finish in time. An earlier version of this function shelled out
// via exec.CommandContext directly with a comment claiming it ran "in its
// own process group" — that was false: CommandContext with no
// SysProcAttr.Setpgid starts the process in THIS worker's process group,
// so context cancellation reaches only the leader pid via os.Process.Kill,
// not any child it spawns, and cmd.Output() still blocks on copying the
// stdout pipe to completion regardless. A wedged driver leaving nvidia-smi
// in uninterruptible sleep — or any grandchild inheriting the stdout pipe —
// would hang gatherLabels forever. Run closes that gap the same way it's
// closed for every drop-in probe; there is no separate, weaker mechanism
// for this one built-in.
//
// Facts are named "gpu<N>.<label>" (nvidiaDeviceKeyPrefix + the index; see
// nvidiaScopedDevices, which derives the devices this source speaks for from
// that same constant) where N is nvidia-smi's own device index (its listing
// order), NOT the configured device name — classifyKey only
// treats a key as device-scoped when its prefix matches a name this host
// actually declared, so on a host whose devices aren't literally named
// "gpu0", "gpu1", ... these facts are dropped (with a warning), never
// silently promoted to host-wide facts misattributed to every device. An
// operator who wants GPU facts attributed to a differently-named device can
// do so with a drop-in probe that knows the mapping.
func nvidiaLabels(ctx context.Context, timeout time.Duration) (facts map[string]string, found, broken bool) {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		// Logged at Debug, not Warn: on a box that genuinely has no
		// NVIDIA GPU and never will, this fires on every single pass
		// forever, and a permanent Warn line for a permanent, expected
		// condition is exactly the kind of alert fatigue that trains an
		// operator to ignore this log stream. gatherLabels decides
		// separately, using its own startup snapshot, whether THIS
		// absence is worth a louder, edge-triggered warning.
		slog.Debug("nvidia-smi not found on PATH; GPU facts unconfirmed this pass", "err", err)
		return nil, false, false
	}

	stdout := &boundedBuffer{limit: maxProbeOutputBytes}
	stderr := &boundedBuffer{limit: maxProbeOutputBytes}
	res := Run(ctx, JobSpec{
		Command: []string{smi,
			"--query-gpu=name,memory.total,memory.free,driver_version", "--format=csv,noheader,nounits"},
		MaxRuntime: timeout,
		Stderr:     stderr,
	}, stdout)

	switch {
	case res.Err != nil:
		slog.Warn("nvidia-smi failed; no GPU labels reported", "err", res.Err)
		return nil, true, true
	case res.Killed:
		reason := res.Reason
		if reason == "" {
			reason = "killed"
		}
		slog.Warn("nvidia-smi timed out; no GPU labels reported", "reason", reason)
		return nil, true, true
	case res.ExitCode != 0:
		slog.Warn("nvidia-smi exited non-zero; no GPU labels reported",
			"exit_code", res.ExitCode, "stderr", strings.TrimSpace(stderr.buf.String()))
		return nil, true, true
	case stdout.truncated:
		slog.Warn("nvidia-smi output exceeded the size cap; no GPU labels reported",
			"limit", maxProbeOutputBytes)
		return nil, true, true
	}

	facts = map[string]string{}
	for i, line := range strings.Split(strings.TrimSpace(stdout.buf.String()), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) != 4 {
			slog.Warn("nvidia-smi line skipped; unexpected format", "line", line)
			continue
		}
		model := strings.TrimSpace(fields[0])
		vram := strings.TrimSpace(fields[1])
		vramFree := strings.TrimSpace(fields[2])
		driver := strings.TrimSpace(fields[3])

		prefix := fmt.Sprintf("%s%d", nvidiaDeviceKeyPrefix, i)
		facts[prefix+".vendor"] = "nvidia"
		facts[prefix+".model"] = model
		// memory.total and memory.free under --format=nounits are bare MiB
		// counts. Emit them suffixed ("M") in the form internal/selector's
		// parseQuantity actually understands (K/M/G/T), not "MiB": a
		// trailing "B" is not one of those suffixes, so "24576MiB" fails to
		// parse as a quantity and Match silently falls back to comparing it
		// as a STRING against the selector's value. That is not a cosmetic
		// difference — "vram>=40G" then compares "9000MiB" against "40G"
		// lexicographically ('9' > '4'), matching a 9GB card against a 40GB
		// request. See probe_nvidia_internal_test.go for the regression
		// test against the real selector package.
		facts[prefix+".vram"] = vram + "M"
		facts[prefix+".vram_free"] = vramFree + "M"
		facts[prefix+".driver"] = driver

		// CUDA version is deliberately NOT emitted here. --query-gpu has no
		// cuda_version field (confirmed against nvidia-smi's own
		// --help-query-gpu listing) — the only place nvidia-smi reports it
		// is the plain-text header of `nvidia-smi -q`/`nvidia-smi` with no
		// --query-gpu at all, once per invocation, not per GPU. Getting it
		// would mean a second exec of nvidia-smi per probe pass plus a
		// fragile text-header parse (a HOST-wide fact bolted onto a
		// per-device CSV row for no reason), against a string NVIDIA does
		// not document as stable across driver releases. A wrong or
		// silently-empty "cuda" label would be worse than no label: a
		// selector like `--select 'cuda>=12'` fails LOUD (rejected at
		// submit, no matching device) if the label never appears, versus
		// failing SILENT if a fragile parse ever produced a stale or empty
		// value. See README's probes section for the same call made in
		// operator-facing docs.
	}
	return facts, true, false
}
