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
