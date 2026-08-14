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
// apply to every device on this host, and facts scoped to one device by
// name. Both maps are always non-nil, even when nothing was gathered, so a
// caller never has to nil-check before ranging or indexing.
type ProbeResult struct {
	Host   map[string]string
	Device map[string]map[string]string

	// Failed reports whether this pass could not confirm at least one
	// source of device-scoped facts: nvidia-smi found on PATH but erroring,
	// timing out, or emitting something that doesn't parse; nvidia-smi
	// simply not found on PATH at all (fix round 2 — see nvidiaLabels' doc
	// comment for the ruling: absence is now treated the same as failure,
	// not as ordinary "no GPUs here"); or a drop-in probe doing either. It
	// is false only when every configured source genuinely ran and
	// confirmed there was nothing to report, or when ProbeDir has no
	// entries and nvidia-smi is on PATH and ran cleanly (empty, but
	// confirmed).
	//
	// This is what lets a caller (see labelsPayload) tell "this device's
	// probe ran and confirmed there is nothing to report" — which
	// ReplaceLabels must be allowed to turn into a clear, per Task 3's own
	// design — apart from "something that can produce device facts broke
	// this pass, so an empty result here proves nothing about the device
	// itself". A single flag, not a per-device or per-probe map: gatherLabels
	// has no way to know in advance which device a probe would have named
	// had it succeeded, so a failure anywhere is treated as inconclusive
	// for every device that came back empty, not just the one nearest the
	// failure.
	Failed bool
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
func (w *Worker) gatherLabels(ctx context.Context) ProbeResult {
	ctx, cancel := context.WithTimeout(ctx, probePassBudget)
	defer cancel()

	deviceNames := make(map[string]struct{}, len(w.cfg.Devices))
	res := ProbeResult{Host: map[string]string{}, Device: map[string]map[string]string{}}
	for _, d := range w.cfg.Devices {
		deviceNames[d.Name] = struct{}{}
		res.Device[d.Name] = map[string]string{}
	}

	merge := func(source string, facts map[string]string) {
		mergeProbeFacts(&res, facts, deviceNames, source)
	}

	merge("builtin", builtinLabels())
	nvFacts, nvFailed := nvidiaLabels(ctx, w.cfg.ProbeTimeout)
	merge("nvidia-smi", nvFacts)
	if nvFailed {
		res.Failed = true
	}

	entries, err := os.ReadDir(w.cfg.ProbeDir)
	if err != nil {
		if !os.IsNotExist(err) {
			// Anything other than "the directory doesn't exist" (permission
			// denied, not-a-directory, ...) is worth a line, but still not
			// fatal: the built-ins above already ran.
			slog.Warn("read probe dir", "dir", w.cfg.ProbeDir, "err", err)
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

	for _, name := range names {
		if err := ctx.Err(); err != nil {
			slog.Warn("probe pass exceeded its budget; remaining probes skipped",
				"dir", w.cfg.ProbeDir, "budget", probePassBudget)
			// A probe skipped for budget reasons never ran at all, so
			// whatever it would have said about any device is simply
			// unknown — indistinguishable, for labelsPayload's purposes,
			// from one that ran and failed outright.
			res.Failed = true
			break
		}

		path := filepath.Join(w.cfg.ProbeDir, name)
		info, err := os.Stat(path)
		if err != nil {
			slog.Warn("stat probe; skipped", "probe", path, "err", err)
			res.Failed = true
			continue
		}
		if info.Mode()&0o111 == 0 {
			continue // not executable: e.g. a README dropped alongside probes
		}

		facts, err := runProbe(ctx, path, w.cfg.ProbeTimeout)
		if err != nil {
			slog.Warn("probe failed; skipped", "probe", path, "err", err)
			res.Failed = true
			continue
		}
		merge(path, facts)
	}
	return res
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
func mergeProbeFacts(res *ProbeResult, facts map[string]string, deviceNames map[string]struct{}, source string) {
	for k, v := range facts {
		switch kind, dev, label := classifyKey(k, deviceNames); kind {
		case keyHost:
			res.Host[k] = v
		case keyDevice:
			if res.Device[dev] == nil {
				res.Device[dev] = map[string]string{}
			}
			res.Device[dev][label] = v
		default: // keyInvalid
			slog.Warn("probe key names no known device or is malformed; dropped", "source", source, "key", k)
		}
	}
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

// nvidiaLabels reports GPU facts from nvidia-smi when it is on PATH. The
// second return, failed, is true whenever this pass could not confirm
// whether GPU facts exist at all — nvidia-smi missing from PATH (fix round
// 2: this used to be treated as ordinary absence, see below for why that
// changed) just as much as nvidia-smi found but erroring, timing out, or
// producing garbage (a non-zero exit, a timeout, output that doesn't parse
// — the "driver upgrade broke nvidia-smi" case, where the binary stays on
// PATH and just stops working: "Failed to initialize NVML: Driver/library
// version mismatch" is the canonical example).
//
// Fix round 2 ruling: an EARLIER version of this function returned
// (nil, false) — "not a failure" — when nvidia-smi was simply not on PATH,
// on the theory that a box with no NVIDIA GPUs simply has no GPU labels.
// Measured against a real controller holding prior GPU labels, that let a
// driver-package upgrade that removes the nvidia-smi BINARY (as opposed to
// merely breaking it) wipe every affected device's vram/gpu facts the
// moment PATH stopped resolving it — identical in effect to nvidia-smi
// erroring, which was already treated as a failure. There is no way for
// gatherLabels to tell "this box never had an NVIDIA GPU" apart from "this
// box's nvidia-smi just vanished" from a single stateless pass, and the
// two failure modes are not the same size: a wipe is fleet-wide and
// immediate (every device loses its GPU labels at once, every vram/gpu
// selector then matches nothing, and every selector job is refused at
// submit across the whole fleet), while treating this as a failure and
// preserving costs at most one device advertising a card that is actually
// gone — recoverable by an operator, and bounded to that device. Ruled:
// preserve. See ProbeResult.Failed and labelsPayload for where that
// preservation actually happens, and their comments for the two
// limitations accepted alongside this ruling (no staleness/expiry on a
// preserved label, and Failed being a single pass-wide bool).
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
// Facts are named "gpu<N>.<label>" where N is nvidia-smi's own device index
// (its listing order), NOT the configured device name — classifyKey only
// treats a key as device-scoped when its prefix matches a name this host
// actually declared, so on a host whose devices aren't literally named
// "gpu0", "gpu1", ... these facts are dropped (with a warning), never
// silently promoted to host-wide facts misattributed to every device. An
// operator who wants GPU facts attributed to a differently-named device can
// do so with a drop-in probe that knows the mapping.
func nvidiaLabels(ctx context.Context, timeout time.Duration) (facts map[string]string, failed bool) {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		// Absent from PATH is now treated the same as "found but broken":
		// see this function's doc comment for the fix-round-2 ruling. Only
		// logged at Debug, not Warn — on the (common) box that genuinely
		// has no NVIDIA GPU and never will, this fires on every single
		// pass forever, and a permanent Warn line for a permanent,
		// expected condition is exactly the kind of alert fatigue that
		// trains an operator to ignore this log stream.
		slog.Debug("nvidia-smi not found on PATH; GPU facts unconfirmed this pass", "err", err)
		return nil, true
	}

	stdout := &boundedBuffer{limit: maxProbeOutputBytes}
	stderr := &boundedBuffer{limit: maxProbeOutputBytes}
	res := Run(ctx, JobSpec{
		Command: []string{smi,
			"--query-gpu=name,memory.total,driver_version", "--format=csv,noheader,nounits"},
		MaxRuntime: timeout,
		Stderr:     stderr,
	}, stdout)

	switch {
	case res.Err != nil:
		slog.Warn("nvidia-smi failed; no GPU labels reported", "err", res.Err)
		return nil, true
	case res.Killed:
		reason := res.Reason
		if reason == "" {
			reason = "killed"
		}
		slog.Warn("nvidia-smi timed out; no GPU labels reported", "reason", reason)
		return nil, true
	case res.ExitCode != 0:
		slog.Warn("nvidia-smi exited non-zero; no GPU labels reported",
			"exit_code", res.ExitCode, "stderr", strings.TrimSpace(stderr.buf.String()))
		return nil, true
	case stdout.truncated:
		slog.Warn("nvidia-smi output exceeded the size cap; no GPU labels reported",
			"limit", maxProbeOutputBytes)
		return nil, true
	}

	facts = map[string]string{}
	for i, line := range strings.Split(strings.TrimSpace(stdout.buf.String()), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) != 3 {
			slog.Warn("nvidia-smi line skipped; unexpected format", "line", line)
			continue
		}
		model := strings.TrimSpace(fields[0])
		vram := strings.TrimSpace(fields[1])
		driver := strings.TrimSpace(fields[2])

		prefix := fmt.Sprintf("gpu%d", i)
		facts[prefix+".vendor"] = "nvidia"
		facts[prefix+".model"] = model
		// memory.total under --format=nounits is a bare MiB count. Emit it
		// suffixed ("M") in the form internal/selector's parseQuantity
		// actually understands (K/M/G/T), not "MiB": a trailing "B" is not
		// one of those suffixes, so "24576MiB" fails to parse as a
		// quantity and Match silently falls back to comparing it as a
		// STRING against the selector's value. That is not a cosmetic
		// difference — "vram>=40G" then compares "9000MiB" against "40G"
		// lexicographically ('9' > '4'), matching a 9GB card against a 40GB
		// request. See probe_nvidia_internal_test.go for the regression
		// test against the real selector package.
		facts[prefix+".vram"] = vram + "M"
		facts[prefix+".driver"] = driver
	}
	return facts, false
}
