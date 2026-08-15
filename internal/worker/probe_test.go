package worker_test

import (
	"context"
	"os"
	"path/filepath"
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
