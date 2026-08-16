package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/selector"
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
		`echo "NVIDIA GeForce RTX 4090, 24576, 20000, 550.54.15"` + "\n" +
		`echo "NVIDIA A100-SXM4-80GB, 81920, 81000, 550.54.15"` + "\n" +
		`echo "Small Card, 9000, 500, 550.54.15"` + "\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	facts, found, broken := nvidiaLabels(context.Background(), 5*time.Second)
	if !found || broken {
		t.Fatalf("expected found=true, broken=false for a working nvidia-smi; got found=%v broken=%v", found, broken)
	}

	want := map[string]string{
		"gpu0.vendor":    "nvidia",
		"gpu0.model":     "NVIDIA GeForce RTX 4090",
		"gpu0.vram":      "24576M",
		"gpu0.vram_free": "20000M",
		"gpu0.driver":    "550.54.15",
		"gpu1.vendor":    "nvidia",
		"gpu1.model":     "NVIDIA A100-SXM4-80GB",
		"gpu1.vram":      "81920M",
		"gpu1.vram_free": "81000M",
		"gpu1.driver":    "550.54.15",
		"gpu2.vendor":    "nvidia",
		"gpu2.model":     "Small Card",
		"gpu2.vram":      "9000M",
		"gpu2.vram_free": "500M",
		"gpu2.driver":    "550.54.15",
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

// TestNvidiaLabelsAbsentReportsNotFound pins the fix-round-3 shape:
// exec.LookPath failing (no nvidia-smi on PATH at all) reports found=false,
// broken=false — nvidiaLabels itself no longer decides whether this counts
// as a "failure" worth preserving a device's stale labels for; that
// decision moved to gatherLabels, which needs to know SEPARATELY whether
// nvidia-smi was ever seen present by this worker before treating an
// absence as meaningful (see gatherLabels' own fix-round-3 comment, and
// TestPushLabelsPreservesGPULabelsWhenNvidiaSmiBinaryDisappears /
// TestPushLabelsClearsGPULabelsOnAHostThatNeverHadNvidiaSmi for the two
// end-to-end behaviors this split makes possible).
func TestNvidiaLabelsAbsentReportsNotFound(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	facts, found, broken := nvidiaLabels(context.Background(), 5*time.Second)
	if facts != nil {
		t.Errorf("expected nil facts when nvidia-smi is absent, got %#v", facts)
	}
	if found {
		t.Error("nvidia-smi absent from PATH must report found=false")
	}
	if broken {
		t.Error("nvidia-smi absent from PATH must never report broken=true — broken only applies once found is true")
	}
}

// TestNvidiaLabelsPresentButBrokenReportsFoundAndBroken is the OTHER half:
// nvidia-smi present on PATH but exiting non-zero — the realistic shape of
// "a driver upgrade broke nvidia-smi" (e.g. "Failed to initialize NVML:
// Driver/library version mismatch") — must report found=true, broken=true,
// unconditionally a failure regardless of gatherLabels' startup-seen state
// (unlike plain absence, being found-but-broken never depends on history).
func TestNvidiaLabelsPresentButBrokenReportsFoundAndBroken(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "nvidia-smi")
	script := "#!/bin/sh\necho 'Failed to initialize NVML' >&2\nexit 1\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	facts, found, broken := nvidiaLabels(context.Background(), 5*time.Second)
	if facts != nil {
		t.Errorf("a broken nvidia-smi must report no facts, got %#v", facts)
	}
	if !found {
		t.Error("nvidia-smi located on PATH must report found=true even though the run itself failed")
	}
	if !broken {
		t.Error("nvidia-smi present but exiting non-zero must report broken=true")
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
		`echo "Big Card, 81920, 80000, 550.54.15"` + "\n" + // 80G
		`echo "Small Card, 9000, 8000, 550.54.15"` + "\n" // ~8.79G, the review's own example
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	facts, _, _ := nvidiaLabels(context.Background(), 5*time.Second)
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

	// vram_free (fix round 4 — free VRAM was omitted entirely) must be the
	// exact same suffixed-quantity shape as vram, for the exact same
	// reason: a bare "MiB" suffix would silently fall back to string
	// comparison and let a selector match the wrong card.
	bigFree := facts["gpu0.vram_free"]
	smallFree := facts["gpu1.vram_free"]
	if bigFree != "80000M" || smallFree != "8000M" {
		t.Fatalf("unexpected vram_free facts: gpu0=%q gpu1=%q", bigFree, smallFree)
	}
	freeSel, err := selector.Parse("vram_free>=40G")
	if err != nil {
		t.Fatal(err)
	}
	if !freeSel.Match(map[string]string{"vram_free": bigFree}) {
		t.Errorf("selector vram_free>=40G should match a card with %s free", bigFree)
	}
	if freeSel.Match(map[string]string{"vram_free": smallFree}) {
		t.Errorf("selector vram_free>=40G must NOT match a card with only %s free", smallFree)
	}
}
