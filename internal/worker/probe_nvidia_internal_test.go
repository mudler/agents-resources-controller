package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/selector"
)

// TestNvidiaLabelsParsesPresentDriver exercises the path that only runs when
// nvidia-smi is actually on PATH — which no CI or dev box can be assumed to
// have — via a fake nvidia-smi script, so the index-based naming and the
// MiB-to-suffixed-quantity conversion get real, committed coverage
// regardless of the hardware this happens to run on. An earlier round of
// this task wrote this exact check, ran it, and then deleted it before
// committing; that left the one code path device selectors actually match
// against with zero coverage. This file replaces that manual, throwaway
// check permanently.
func TestNvidiaLabelsParsesPresentDriver(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "nvidia-smi")
	script := "#!/bin/sh\n" +
		`echo "NVIDIA GeForce RTX 4090, 24576, 550.54.15"` + "\n" +
		`echo "NVIDIA A100-SXM4-80GB, 81920, 550.54.15"` + "\n" +
		`echo "Small Card, 9000, 550.54.15"` + "\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	facts, _ := nvidiaLabels(context.Background(), 5*time.Second)

	want := map[string]string{
		"gpu0.vendor": "nvidia",
		"gpu0.model":  "NVIDIA GeForce RTX 4090",
		"gpu0.vram":   "24576M",
		"gpu0.driver": "550.54.15",
		"gpu1.vendor": "nvidia",
		"gpu1.model":  "NVIDIA A100-SXM4-80GB",
		"gpu1.vram":   "81920M",
		"gpu1.driver": "550.54.15",
		"gpu2.vendor": "nvidia",
		"gpu2.model":  "Small Card",
		"gpu2.vram":   "9000M",
		"gpu2.driver": "550.54.15",
	}
	for k, v := range want {
		if facts[k] != v {
			t.Errorf("facts[%q] = %q, want %q", k, facts[k], v)
		}
	}
	if len(facts) != len(want) {
		t.Errorf("got %d facts, want %d: %#v", len(facts), len(want), facts)
	}
}

// TestNvidiaLabelsAbsentIsTreatedAsAFailure pins the fix-round-2 ruling:
// exec.LookPath failing (no nvidia-smi on PATH at all) now reports
// failed=true, the same as nvidia-smi being present but broken. This
// INVERTS the original Task 5 stance (ordinary absence, not a failure),
// deliberately: a real controller holding prior GPU labels for a device
// loses them the moment a driver-package upgrade removes the nvidia-smi
// BINARY (as opposed to merely breaking it) if this case were still
// treated as "no failure" — see TestPushLabelsPreservesGPULabelsWhenNvidiaSmiBinaryIsMissing
// for the end-to-end proof. gatherLabels has no way to tell "this box
// never had an NVIDIA GPU" apart from "this box's nvidia-smi just
// vanished" from a single stateless pass, and a fleet-wide wipe (every
// selector job refused at submit) was judged worse than one device
// advertising a card that is actually gone.
func TestNvidiaLabelsAbsentIsTreatedAsAFailure(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	facts, failed := nvidiaLabels(context.Background(), 5*time.Second)
	if facts != nil {
		t.Errorf("expected nil facts when nvidia-smi is absent, got %#v", facts)
	}
	if !failed {
		t.Error("nvidia-smi absent from PATH must be reported as a failure (fix round 2 ruling), not ordinary absence")
	}
}

// TestNvidiaLabelsPresentButBrokenIsAFailure is the OTHER half: nvidia-smi
// present on PATH but exiting non-zero — the realistic shape of "a driver
// upgrade broke nvidia-smi" (e.g. "Failed to initialize NVML: Driver/library
// version mismatch") — must be reported as failed=true, not treated the
// same as ordinary absence. See ProbeResult.Failed for why the distinction
// matters: only this case may withhold a device's stale facts from being
// wiped by an empty result.
func TestNvidiaLabelsPresentButBrokenIsAFailure(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "nvidia-smi")
	script := "#!/bin/sh\necho 'Failed to initialize NVML' >&2\nexit 1\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	facts, failed := nvidiaLabels(context.Background(), 5*time.Second)
	if facts != nil {
		t.Errorf("a broken nvidia-smi must report no facts, got %#v", facts)
	}
	if !failed {
		t.Error("nvidia-smi present but exiting non-zero must be reported as a failure")
	}
}

// TestProbedVRAMComparesCorrectlyThroughSelector is the elevated review
// finding, made concrete: the OLD "<n>MiB" form does not parse as a
// selector.parseQuantity (its trailing byte is 'B', not one of K/M/G/T), so
// the comparison silently fell back to lexicographic STRING ordering.
// Concretely, "9000MiB" >= "40G" was true as strings ('9' > '4'), so a
// selector asking for vram>=40G would have matched a 9000 MiB card — the
// exact wrong-card scheduling bug this whole label system exists to
// prevent. This test proves the fixed "<n>M" form compares NUMERICALLY and
// correctly through the real internal/selector package, not just that it
// looks plausible in isolation.
func TestProbedVRAMComparesCorrectlyThroughSelector(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "nvidia-smi")
	script := "#!/bin/sh\n" +
		`echo "Big Card, 81920, 550.54.15"` + "\n" + // 80G
		`echo "Small Card, 9000, 550.54.15"` + "\n" // ~8.79G, the review's own example
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	facts, _ := nvidiaLabels(context.Background(), 5*time.Second)
	bigVRAM := facts["gpu0.vram"]
	smallVRAM := facts["gpu1.vram"]
	if bigVRAM != "81920M" || smallVRAM != "9000M" {
		t.Fatalf("unexpected vram facts: gpu0=%q gpu1=%q", bigVRAM, smallVRAM)
	}

	sel, err := selector.Parse("vram>=40G")
	if err != nil {
		t.Fatal(err)
	}
	if !sel.Match(map[string]string{"vram": bigVRAM}) {
		t.Errorf("selector vram>=40G should match an 80G card (%s)", bigVRAM)
	}
	if sel.Match(map[string]string{"vram": smallVRAM}) {
		t.Errorf("selector vram>=40G must NOT match a %s card — this is the exact regression this test guards", smallVRAM)
	}

	// And a threshold the small card genuinely clears, so this isn't
	// accidentally testing "always false".
	loose, err := selector.Parse("vram>=8G")
	if err != nil {
		t.Fatal(err)
	}
	if !loose.Match(map[string]string{"vram": smallVRAM}) {
		t.Errorf("selector vram>=8G should match a %s card", smallVRAM)
	}
}
