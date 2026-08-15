package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestProbePassIsBoundedAsAWhole guards the fix for the review finding that
// gatherLabels had no budget on the pass as a whole: N drop-in probes each
// capped at their own ProbeTimeout still serialise to N*ProbeTimeout, which
// at 20 probes and the 5s default is 100s. Here every probe's own timeout
// (2s) is deliberately larger than the pass budget (300ms), so if the pass
// were only bounded by summing individual timeouts this test would take
// several seconds; it must instead finish close to the pass budget with the
// later probes simply skipped.
func TestProbePassIsBoundedAsAWhole(t *testing.T) {
	orig := probePassBudget
	probePassBudget = 300 * time.Millisecond
	t.Cleanup(func() { probePassBudget = orig })

	dir := t.TempDir()
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("%02d-hang.sh", i*10+10)
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	w := New(Config{
		Host:         "box",
		Devices:      []DeviceConfig{{Name: "gpu0"}},
		ProbeDir:     dir,
		ProbeTimeout: 2 * time.Second,
	})

	start := time.Now()
	w.gatherLabels(context.Background())
	elapsed := time.Since(start)

	if elapsed >= 5*time.Second {
		t.Errorf("probe pass took %s; must be bounded as a whole, not by summing each probe's own timeout", elapsed)
	}
}

// TestProbeSkippedForBudgetPreservesTheDevicesItSpeaksFor: a probe the pass
// budget cut short never ran at all, which says exactly as little about the
// devices it covers as one that ran and failed. 05-slow.sh burns the whole
// budget on the second pass, so 10-gpu.sh — which reported gpu0's facts on
// the first pass — is skipped without ever being executed. gpu0 must come
// back unconfirmed (preserved, not cleared), and gpu1, which no probe here
// has ever named, must not be dragged along with it.
func TestProbeSkippedForBudgetPreservesTheDevicesItSpeaksFor(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	marker := filepath.Join(state, "ran")

	if err := os.WriteFile(filepath.Join(dir, "05-slow.sh"),
		[]byte("#!/bin/sh\nif [ -f "+marker+" ]; then sleep 5; fi\ntouch "+marker+"\necho '{}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "10-gpu.sh"),
		[]byte("#!/bin/sh\necho '{\"gpu0.vram\":\"80G\"}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	w := New(Config{
		Host:         "box",
		Devices:      []DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir:     dir,
		ProbeTimeout: 2 * time.Second,
	})

	if got := w.gatherLabels(context.Background()).Device["gpu0"]["vram"]; got != "80G" {
		t.Fatalf("sanity: the first pass must capture gpu0's fact; got %q", got)
	}

	orig := probePassBudget
	probePassBudget = 300 * time.Millisecond
	t.Cleanup(func() { probePassBudget = orig })

	res := w.gatherLabels(context.Background())
	if !res.Unconfirmed["gpu0"] {
		t.Error("a probe skipped for budget never ran, so the devices it speaks for must be preserved, not cleared")
	}
	if res.Unconfirmed["gpu1"] {
		t.Error("no probe in this directory has ever named gpu1; a budget overrun must not freeze it too")
	}
}
