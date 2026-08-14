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
