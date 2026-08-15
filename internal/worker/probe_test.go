package worker_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func writeProbe(t *testing.T, dir, name, body string) {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755))
}

func TestProbeOutputBecomesHostLabels(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-facts.sh", `echo '{"vendor":"nvidia","cuda":"12.4"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"])
	require.Equal(t, "12.4", res.Host["cuda"])
}

func TestDeviceScopedProbeKeys(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-vram.sh", `echo '{"gpu0.vram":"80G","gpu1.vram":"24G"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "80G", res.Device["gpu0"]["vram"])
	require.Equal(t, "24G", res.Device["gpu1"]["vram"])
	require.NotContains(t, res.Host, "gpu0.vram", "a device-scoped key is not a host fact")
}

func TestFailingProbeIsSkippedNotFatal(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-broken.sh", `echo boom >&2; exit 1`)
	writeProbe(t, dir, "20-good.sh", `echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"], "a broken probe must not lose a good one")
}

func TestGarbageOutputIsSkipped(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-garbage.sh", `echo 'not json at all'`)
	writeProbe(t, dir, "20-good.sh", `echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"])
	require.NotContains(t, res.Host, "not")
}

func TestHangingProbeIsTimedOut(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-hang.sh", `sleep 30`)
	writeProbe(t, dir, "20-good.sh", `echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:      []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir:     dir,
		ProbeTimeout: 300 * time.Millisecond,
	})

	start := time.Now()
	res := w.GatherLabelsForTest(context.Background())
	require.Less(t, time.Since(start), 10*time.Second, "a hanging probe must not stall the pass")
	require.Equal(t, "nvidia", res.Host["vendor"])
}

func TestNonScalarValuesAreSkipped(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-nested.sh", `echo '{"ok":"yes","nested":{"a":1},"list":[1,2]}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "yes", res.Host["ok"])
	require.NotContains(t, res.Host, "nested")
	require.NotContains(t, res.Host, "list")
}

func TestNumbersAndBoolsBecomeStrings(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-types.sh", `echo '{"cores":32,"ecc":true}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "32", res.Host["cores"])
	require.Equal(t, "true", res.Host["ecc"])
}

func TestMissingProbeDirIsNotAnError(t *testing.T) {
	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: filepath.Join(t.TempDir(), "does-not-exist"),
	})

	res := w.GatherLabelsForTest(context.Background())
	require.NotNil(t, res.Host, "built-ins still ran")
}

// TestOversizedProbeOutputIsSkipped guards the fix for the review finding
// that probe output was buffered unbounded: a probe emitting 200MB was
// measured fully buffered in 1.57s, well inside the default 5s timeout, so
// a slow probe timeout alone did nothing to bound memory. A misbehaving
// probe's output must be capped, and capping it must not cost a sibling
// probe its labels.
func TestOversizedProbeOutputIsSkipped(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-huge.sh", `yes '{"x":1}' | head -c 200000`)
	writeProbe(t, dir, "20-good.sh", `echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"], "an oversized probe must not block a good one")
	require.NotContains(t, res.Host, "x")
}

// TestUnknownDeviceScopedKeyIsDroppedNotHostFact guards the fix for the
// review finding that a device-scoped key naming a device this host does
// not declare used to fall through to a HOST fact, applied to every
// device. On a host declaring "a100-0"/"rtx-0", a probe (or the nvidia-smi
// built-in) emitting "gpu0.model"/"gpu1.model" would silently tell every
// device it was BOTH GPUs at once. The key must be dropped instead.
func TestUnknownDeviceScopedKeyIsDroppedNotHostFact(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-mismatch.sh", `echo '{"gpu2.model":"RTX 4090","vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"])
	require.NotContains(t, res.Host, "gpu2.model", "an unknown device-scoped key must not leak into host facts")
	require.NotContains(t, res.Device["gpu0"], "model")
	require.NotContains(t, res.Device["gpu1"], "model")
}

// TestMalformedDottedKeysAreDropped guards the ruled-in minor finding: a key
// that is empty, or dotted with an empty prefix/suffix, is neither a valid
// host key nor a valid device key and must be dropped, not accepted as a
// host fact.
func TestMalformedDottedKeysAreDropped(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-malformed.sh", `echo '{"":"x",".lead":"y","trail.":"z","vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"])
	require.NotContains(t, res.Host, "")
	require.NotContains(t, res.Host, ".lead")
	require.NotContains(t, res.Host, "trail.")
}

// TestBrokenProbeMarksOnlyTheDevicesItSpeaksFor pins the per-source
// attribution that replaced the pass-wide Failed flag: a drop-in that
// reported gpu0's facts and then broke leaves gpu0 unconfirmed — preserve,
// do not clear, unchanged — while gpu1, which that probe never said a word
// about, stays confirmed and keeps refreshing.
func TestBrokenProbeMarksOnlyTheDevicesItSpeaksFor(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	marker := filepath.Join(state, "ran")
	writeProbe(t, dir, "10-gpu.sh",
		"if [ -f "+marker+" ]; then echo broken >&2; exit 1; fi\n"+
			"touch "+marker+"\n"+
			`echo '{"gpu0.vram":"80G"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	first := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "80G", first.Device["gpu0"]["vram"], "sanity: the first pass must capture gpu0's fact")
	require.Empty(t, first.Unconfirmed, "nothing failed on the first pass")

	second := w.GatherLabelsForTest(context.Background())
	require.True(t, second.Unconfirmed["gpu0"], "the device the broken probe reported last time is unconfirmed")
	require.False(t, second.Unconfirmed["gpu1"],
		"gpu1 was never named by the broken probe: freezing it too is the pass-wide behaviour this replaced")
}

// TestProbeThatNeverSucceededSpeaksForNoDevice is the measured defect at the
// gatherLabels layer: a drop-in that has never once worked in this process
// has told this worker nothing about any device, so its failure must not
// make any device's facts unconfirmed — otherwise a single permanently
// broken script freezes every host fact on every device of the box forever,
// which is exactly how disk_free_bytes=72G stayed advertised on a full one.
func TestProbeThatNeverSucceededSpeaksForNoDevice(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "50-custom.sh", `echo 'jq: command not found' >&2; exit 127`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.NotEmpty(t, res.Host, "the built-ins ran and their host facts must be reported")
	require.Empty(t, res.Unconfirmed,
		"a probe that has never reported a device fact speaks for no device; its failure must freeze nothing")
}

// TestPreservationLastsAsLongAsTheOutage is the review-round-1 finding:
// every other test here stops at the FIRST failing pass, so a scope memory
// that is consulted but never carried forward would preserve gpu0 once and
// then, having forgotten what the broken script covers, send gpu0 as a
// present empty map on the very next pass — ReplaceLabels wipes it, one
// probe interval late, with the whole suite green. Real outages last many
// intervals (a driver upgrade, a script broken until someone notices), so
// preservation is only worth anything if it holds pass after pass.
func TestPreservationLastsAsLongAsTheOutage(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	marker := filepath.Join(state, "ran")
	writeProbe(t, dir, "10-gpu.sh",
		"if [ -f "+marker+" ]; then echo broken >&2; exit 1; fi\n"+
			"touch "+marker+"\n"+
			`echo '{"gpu0.vram":"80G"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	require.Equal(t, "80G", w.GatherLabelsForTest(context.Background()).Device["gpu0"]["vram"],
		"sanity: the first pass must capture gpu0's fact")

	for pass := 2; pass <= 5; pass++ {
		res := w.GatherLabelsForTest(context.Background())
		require.True(t, res.Unconfirmed["gpu0"],
			"pass %d: gpu0 must still be preserved — the probe that covers it has been failing since pass 2, "+
				"and an outage lasting longer than one probe interval is the normal case, not the exception", pass)
		require.False(t, res.Unconfirmed["gpu1"], "pass %d: gpu1 was never covered by that probe", pass)
	}
}

// TestHostOnlyProbeSpeaksForNoDeviceEvenAfterSucceeding is the central claim
// of this fix, stated as a test rather than as prose in a comment: a drop-in
// that SUCCEEDS and reports only host-scoped keys has named no device, so
// when it later breaks it must not freeze any. This is the exact shape of
// the script in the measured defect — disk_free_bytes is a host key — and
// the case no other test covers: the never-succeeded test has no success
// history at all, and the end-to-end test's broken script emits a device
// key.
func TestHostOnlyProbeSpeaksForNoDeviceEvenAfterSucceeding(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	marker := filepath.Join(state, "ran")
	writeProbe(t, dir, "50-custom.sh",
		"if [ -f "+marker+" ]; then echo broken >&2; exit 1; fi\n"+
			"touch "+marker+"\n"+
			`echo '{"rack":"b12","disk_free_bytes":"72000000000"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	first := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "b12", first.Host["rack"], "sanity: the first pass must capture the host-scoped facts")
	require.Empty(t, first.Unconfirmed)

	second := w.GatherLabelsForTest(context.Background())
	require.Empty(t, second.Unconfirmed,
		"a probe whose successful run named no device speaks for none of them: its failure must freeze nothing, "+
			"or one broken host-fact script freezes the whole box again")
	require.NotContains(t, second.Host, "rack",
		"and its own host facts are gone, not preserved — a label that vanishes fails a selector loudly, "+
			"a stale one routes a job onto the wrong box")
}

// TestClearingProbeWarnsOncePerEpisode: when a failing drop-in has no
// remembered scope, this worker preserves nothing for it and whatever it
// used to report is cleared. That is the ruling, but it must not happen
// silently — an operator otherwise sees "probe failed; skipped" on the
// worker and, quite separately, labels disappearing and submits being
// rejected, with nothing tying the two together and no notify event for
// labels. Once per episode, not once per pass: an outage lasts many
// intervals, and a line per pass is how a log stream stops being read.
func TestClearingProbeWarnsOncePerEpisode(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	ok := filepath.Join(state, "ok")
	writeProbe(t, dir, "50-custom.sh",
		"if [ -f "+ok+" ]; then echo '{\"rack\":\"b12\"}'; exit 0; fi\n"+
			"echo broken >&2; exit 1")

	var logs logCapture
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	const phrase = "cleared rather than preserved"
	w.GatherLabelsForTest(context.Background())
	require.Equal(t, 1, strings.Count(logs.String(), phrase),
		"the first pass of an episode must say that this probe's facts are being cleared, not preserved")
	require.Contains(t, logs.String(), "50-custom.sh", "the line must name the probe that failed")
	require.Contains(t, logs.String(), "gpu0", "and the devices the decision affects")
	require.Contains(t, logs.String(), "gpu1")

	for i := 0; i < 3; i++ {
		w.GatherLabelsForTest(context.Background())
	}
	require.Equal(t, 1, strings.Count(logs.String(), phrase),
		"the same episode continuing must not re-log every pass")

	// It recovers, then breaks again: a NEW episode gets its own line.
	require.NoError(t, os.WriteFile(ok, nil, 0o644))
	w.GatherLabelsForTest(context.Background())
	require.Equal(t, 1, strings.Count(logs.String(), phrase), "a successful pass logs nothing of its own")

	require.NoError(t, os.Remove(ok))
	w.GatherLabelsForTest(context.Background())
	require.Equal(t, 2, strings.Count(logs.String(), phrase),
		"a new episode after a recovery must warn again rather than stay silent on a stale marker")
}

// TestProbeThatStopsNamingADeviceStopsSpeakingForIt: the memory of which
// devices a source covers is REPLACED on every successful run, not
// accumulated. A probe that reported gpu0 once, then ran cleanly and
// reported nothing (the ordinary "that fact is gone" case, which clears
// gpu0's labels), no longer speaks for gpu0 — so when it later breaks, gpu0
// must not be frozen on the strength of a report two passes stale.
func TestProbeThatStopsNamingADeviceStopsSpeakingForIt(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	first := filepath.Join(state, "first")
	second := filepath.Join(state, "second")
	writeProbe(t, dir, "10-gpu.sh",
		"if [ -f "+second+" ]; then echo broken >&2; exit 1; fi\n"+
			"if [ -f "+first+" ]; then touch "+second+"; echo '{}'; exit 0; fi\n"+
			"touch "+first+"\n"+
			`echo '{"gpu0.vram":"80G"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	require.Equal(t, "80G", w.GatherLabelsForTest(context.Background()).Device["gpu0"]["vram"], "sanity: pass 1 names gpu0")
	require.Empty(t, w.GatherLabelsForTest(context.Background()).Unconfirmed, "sanity: pass 2 succeeds, naming nothing")

	third := w.GatherLabelsForTest(context.Background())
	require.Empty(t, third.Unconfirmed,
		"the probe's last successful run named no device, so its failure now speaks for none either")
}

// TestUnreadableProbeDirPreservesTheDevicesItsProbesSpeakFor: a probe
// directory that cannot be read is every drop-in in it failing at once, not
// a directory that ran and confirmed there is nothing to report. A `chmod
// 000 /etc/rc/probe.d` (or a bad umask in config management) must not clear
// every device's detected labels across the fleet.
func TestUnreadableProbeDirPreservesTheDevicesItsProbesSpeakFor(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions do not deny reads")
	}
	dir := t.TempDir()
	writeProbe(t, dir, "10-gpu.sh", `echo '{"gpu0.vram":"80G"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})
	require.Equal(t, "80G", w.GatherLabelsForTest(context.Background()).Device["gpu0"]["vram"], "sanity: pass 1 works")

	require.NoError(t, os.Chmod(dir, 0o000))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) }) // or TempDir's own cleanup fails

	res := w.GatherLabelsForTest(context.Background())
	require.NotEmpty(t, res.Host, "the built-ins still ran")
	require.True(t, res.Unconfirmed["gpu0"], "the probe that speaks for gpu0 never got to run")
	require.False(t, res.Unconfirmed["gpu1"], "no probe has ever spoken for gpu1")
}

// TestUnreadableProbeDirDoesNotRecloudASourceThatRanFine: the unreadable
// directory belongs to the drop-ins in it and to nothing else. nvidia-smi
// ran perfectly well on this pass, so re-clouding the devices IT covers
// because a different source could not be reached would be this task's own
// bug — one source's failure making another source's facts doubtful —
// reappearing in the branch added to fix a neighbouring hole.
func TestUnreadableProbeDirDoesNotRecloudASourceThatRanFine(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions do not deny reads")
	}
	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "nvidia-smi"),
		[]byte("#!/bin/sh\necho \"NVIDIA A100-SXM4-80GB, 81920, 80000, 550.54.15\"\n"), 0o755))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	dir := t.TempDir()
	writeProbe(t, dir, "10-other.sh", `echo '{"gpu1.serial":"SN123"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})
	first := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "81920M", first.Device["gpu0"]["vram"], "sanity: nvidia-smi covers gpu0")
	require.Equal(t, "SN123", first.Device["gpu1"]["serial"], "sanity: the drop-in covers gpu1")

	require.NoError(t, os.Chmod(dir, 0o000))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	res := w.GatherLabelsForTest(context.Background())
	require.True(t, res.Unconfirmed["gpu1"], "the drop-in that covers gpu1 never got to run")
	require.False(t, res.Unconfirmed["gpu0"],
		"nvidia-smi ran fine this pass and still reports gpu0: an unreadable probe directory says nothing about it")
	require.Equal(t, "81920M", res.Device["gpu0"]["vram"], "sanity: gpu0's facts are fresh, not stale")
}

// TestAbsentProbeDirClearsNormally is the complement to the unreadable case,
// and the reason the two branches differ: a probe directory that is not
// there is a legitimate, confirmed configuration — this host has no drop-in
// probes — not a source that failed. Its probes' facts clear the ordinary
// way, exactly as a probe that stops reporting a key does.
func TestAbsentProbeDirClearsNormally(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "probe.d")
	require.NoError(t, os.Mkdir(dir, 0o755))
	writeProbe(t, dir, "10-gpu.sh", `echo '{"gpu0.vram":"80G"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})
	require.Equal(t, "80G", w.GatherLabelsForTest(context.Background()).Device["gpu0"]["vram"], "sanity: pass 1 works")

	require.NoError(t, os.RemoveAll(dir))

	res := w.GatherLabelsForTest(context.Background())
	require.NotEmpty(t, res.Host, "the built-ins still ran")
	require.Empty(t, res.Unconfirmed,
		"a probe directory that does not exist is not a failure: gpu0's facts must clear the ordinary way, "+
			"or removing probe_dir would freeze every device on the host forever")
}

// TestBrokenNvidiaSmiPreservesOnlyDevicesItCouldEverLabel pins
// nvidiaScopedDevices against the naming rule it is derived from: nvidiaLabels
// emits "gpu<N>.<label>" and classifyKey keeps such a key only for a device
// declared under that exact name, so nvidia-smi breaking says nothing at all
// about a device named "a100-0" — it could not have labelled it even when it
// worked. Freezing that device (as the pass-wide flag did) costs it every
// host fact for as long as the driver stays broken, for no gain.
func TestBrokenNvidiaSmiPreservesOnlyDevicesItCouldEverLabel(t *testing.T) {
	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "nvidia-smi"),
		[]byte("#!/bin/sh\necho 'Failed to initialize NVML: Driver/library version mismatch' >&2\nexit 1\n"), 0o755))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices: []worker.DeviceConfig{{Name: "gpu0"}, {Name: "a100-0"}},
	})

	res := w.GatherLabelsForTest(context.Background())
	require.True(t, res.Unconfirmed["gpu0"],
		"a broken nvidia-smi leaves the gpu<N>-named devices unconfirmed, exactly as before")
	require.False(t, res.Unconfirmed["a100-0"],
		"nvidia-smi's keys could never have landed on a device named a100-0, so its failure must not freeze it")
}

// TestBrokenNvidiaSmiPrefersTheDevicesItActuallyReported: once nvidia-smi has
// succeeded in this process, the devices it NAMED are better evidence than
// the devices it could theoretically name. A box declaring gpu0..gpu3 with
// one card installed must not have gpu1's host facts frozen because
// nvidia-smi — which has only ever reported gpu0 — broke.
func TestBrokenNvidiaSmiPrefersTheDevicesItActuallyReported(t *testing.T) {
	binDir := t.TempDir()
	state := t.TempDir()
	marker := filepath.Join(state, "ran")
	fake := "#!/bin/sh\n" +
		"if [ -f " + marker + " ]; then echo 'NVML mismatch' >&2; exit 1; fi\n" +
		"touch " + marker + "\n" +
		`echo "NVIDIA A100-SXM4-80GB, 81920, 80000, 550.54.15"` + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "nvidia-smi"), []byte(fake), 0o755))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices: []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
	})

	first := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "81920M", first.Device["gpu0"]["vram"], "sanity: the fake nvidia-smi reported one card, gpu0")
	require.Empty(t, first.Device["gpu1"], "sanity: only one card was listed")

	second := w.GatherLabelsForTest(context.Background())
	require.True(t, second.Unconfirmed["gpu0"], "gpu0's GPU facts are unconfirmed while nvidia-smi is broken")
	require.False(t, second.Unconfirmed["gpu1"],
		"nvidia-smi has never reported a second card, so breaking says nothing about gpu1")
}

// TestStderrWarningDoesNotLoseStdoutLabels guards the fix for the review
// finding that Run's default combined stdout+stderr stream meant a probe
// writing one diagnostic line to stderr and exiting 0 lost every label it
// produced: the merged buffer no longer parsed as a single JSON object.
// stdout must be captured separately from stderr for probes.
func TestStderrWarningDoesNotLoseStdoutLabels(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-warn.sh", `echo warning >&2; echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"], "a stderr warning must not lose the probe's stdout labels")
}
