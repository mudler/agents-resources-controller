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
}

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
	deviceNames := make(map[string]struct{}, len(w.cfg.Devices))
	res := ProbeResult{Host: map[string]string{}, Device: map[string]map[string]string{}}
	for _, d := range w.cfg.Devices {
		deviceNames[d.Name] = struct{}{}
		res.Device[d.Name] = map[string]string{}
	}

	merge := func(facts map[string]string) {
		mergeProbeFacts(&res, facts, deviceNames)
	}

	merge(builtinLabels())
	merge(nvidiaLabels(ctx, w.cfg.ProbeTimeout))

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
		path := filepath.Join(w.cfg.ProbeDir, name)
		info, err := os.Stat(path)
		if err != nil {
			slog.Warn("stat probe; skipped", "probe", path, "err", err)
			continue
		}
		if info.Mode()&0o111 == 0 {
			continue // not executable: e.g. a README dropped alongside probes
		}

		facts, err := runProbe(ctx, path, w.cfg.ProbeTimeout)
		if err != nil {
			slog.Warn("probe failed; skipped", "probe", path, "err", err)
			continue
		}
		merge(facts)
	}
	return res
}

// mergeProbeFacts applies one probe's flat fact map into res. A key of the
// form "<device-name>.<label>" — where <device-name> matches one of this
// host's configured devices — is a device fact; every other key, dotted or
// not, is a host fact applied to every device.
func mergeProbeFacts(res *ProbeResult, facts map[string]string, deviceNames map[string]struct{}) {
	for k, v := range facts {
		if dev, label, ok := splitDeviceKey(k, deviceNames); ok {
			if res.Device[dev] == nil {
				res.Device[dev] = map[string]string{}
			}
			res.Device[dev][label] = v
			continue
		}
		res.Host[k] = v
	}
}

// splitDeviceKey reports whether key targets one specific device: the
// substring before its first "." must name a device this host actually
// declares, otherwise the key is left whole as a host fact. This is what
// keeps an incidental dot in a host-scoped key (say, a probe that ever
// emitted "os.kernel") from being misread as aimed at a device called "os".
func splitDeviceKey(key string, deviceNames map[string]struct{}) (dev, label string, ok bool) {
	i := strings.Index(key, ".")
	if i <= 0 || i == len(key)-1 {
		return "", "", false
	}
	prefix := key[:i]
	if _, known := deviceNames[prefix]; !known {
		return "", "", false
	}
	return prefix, key[i+1:], true
}

// runProbe executes one probe under the same process-group supervision a job
// or a lifecycle hook gets (Run, in exec.go): its own process group, killed
// whole on timeout, so a wedged probe never leaks a process nor stalls the
// pass past its own budget. A non-zero exit, a kill (timeout or otherwise),
// or output that doesn't parse as a single flat JSON object is reported as
// an error; the caller logs it and moves on to the next probe.
func runProbe(ctx context.Context, path string, timeout time.Duration) (map[string]string, error) {
	var buf bytes.Buffer
	res := Run(ctx, JobSpec{Command: []string{path}, MaxRuntime: timeout}, &buf)
	if res.Err != nil {
		return nil, fmt.Errorf("run: %w", res.Err)
	}
	if res.Killed {
		reason := res.Reason
		if reason == "" {
			reason = "killed"
		}
		return nil, fmt.Errorf("killed: %s", reason)
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("exited %d", res.ExitCode)
	}
	return parseProbeOutput(path, buf.Bytes())
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

// nvidiaLabels reports GPU facts from nvidia-smi when it is on PATH; when it
// is absent, this returns nil and that is not an error — a box with no
// NVIDIA GPUs has no GPU labels, full stop. When present, it is still run
// under a bounded timeout: a wedged driver making nvidia-smi hang is a real
// failure mode this must survive exactly like any drop-in probe would.
//
// Facts are named "gpu<N>.<label>" where N is nvidia-smi's own device index
// (its listing order), NOT the configured device name — mergeProbeFacts
// only treats a key as device-scoped when its prefix matches a name this
// host actually declared, so on a host whose devices aren't literally named
// "gpu0", "gpu1", ... these land as host facts instead. That is a deliberate
// simplification for this built-in, not a bug: an operator who wants GPU
// facts attributed to a differently-named device can do so with a drop-in
// probe that knows the mapping.
func nvidiaLabels(ctx context.Context, timeout time.Duration) map[string]string {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return nil
	}

	runCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, smi,
		"--query-gpu=name,memory.total,driver_version", "--format=csv,noheader,nounits")
	// Its own process group, same as everything else this file runs — a
	// context-cancel kills the group leader via CommandContext's default
	// Cancel, which is sufficient here since nvidia-smi itself doesn't fork
	// long-lived children the way a hook or job might.
	out, err := cmd.Output()
	if err != nil {
		slog.Warn("nvidia-smi failed; no GPU labels reported", "err", err)
		return nil
	}

	facts := map[string]string{}
	for i, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
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
		facts[prefix+".vram"] = vram + "MiB" // memory.total, --format=nounits: a bare MiB count
		facts[prefix+".driver"] = driver
	}
	return facts
}
