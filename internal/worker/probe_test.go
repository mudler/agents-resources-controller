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
