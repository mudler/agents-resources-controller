package worker

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is the trap carried forward from Task 3: ReplaceLabels treats an
// empty map as "clear this source", and the controller cannot tell "the
// probe ran and found nothing" from "the probe did not run at all". A whole
// pass that produced nothing anywhere (no host facts either) must translate
// to nil here, not {}, or a fleet-wide probe outage on a host with no
// working built-ins at all would wipe every device's detected labels on the
// next push. This is the defensive, essentially-never-hit case; the REAL
// everyday guard is per-device, see the Unconfirmed tests below — a real
// gatherLabels pass almost always has non-empty Host (builtinLabels always
// yields at least "cpus"), which is exactly why a pass-wide-only guard was
// found inert in fix round 1: see
// TestLabelsPayloadOmitsFailedDeviceButKeepsHostAndOtherDevices.
func TestLabelsPayloadNilWhenPassProducedNothing(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{},
		Device: map[string]map[string]string{"gpu0": {}, "gpu1": {}},
	}
	require.Nil(t, labelsPayload(res, map[string]bool{}), "a pass with zero facts anywhere must send nil, not an empty map")
}

// When nothing failed this pass, an empty device result is trustworthy: the
// full snapshot is sent, including devices that themselves got nothing this
// round (an entirely ordinary, legitimate report — that specific device's
// own facts didn't change).
func TestLabelsPayloadIncludesEverythingWhenPassProducedAnything(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{"kernel": "6.8.0"},
		Device: map[string]map[string]string{"gpu0": {"vram": "80G"}, "gpu1": {}},
	}
	out := labelsPayload(res, map[string]bool{})
	require.NotNil(t, out)
	require.Equal(t, map[string]string{"kernel": "6.8.0"}, out[""])
	require.Equal(t, map[string]string{"vram": "80G"}, out["gpu0"])
	require.Empty(t, out["gpu1"], "gpu1's own empty result is sent as-is, not treated as pass failure")
	_, ok := out["gpu1"]
	require.True(t, ok, "an empty-but-trustworthy device is still a PRESENT key (an explicit clear), not omitted")
}

// This is the fix-round-1 finding, pinned directly: builtinLabels() always
// yields at least "cpus", so res.Host is essentially NEVER empty on a real
// host — the old pass-wide "everything empty" guard therefore never fired
// in production no matter what happened to a device's own probes. The
// actual protection has to be per device: gpu0's own facts vanished this
// pass (a source that speaks for it failed — res.Unconfirmed names it), so
// gpu0 must be OMITTED from the output entirely — not sent as {}, which
// ReplaceLabels would still turn into a clear — while gpu1 (whose own facts
// came through fine) and the host-wide key are unaffected.
func TestLabelsPayloadOmitsFailedDeviceButKeepsHostAndOtherDevices(t *testing.T) {
	res := ProbeResult{
		Host:        map[string]string{"cpus": "8"},
		Device:      map[string]map[string]string{"gpu0": {}, "gpu1": {"vram": "40G"}},
		Unconfirmed: map[string]bool{"gpu0": true},
	}
	out := labelsPayload(res, map[string]bool{})
	require.NotNil(t, out)
	require.Equal(t, map[string]string{"cpus": "8"}, out[""], "host-wide facts are unaffected by a device probe failure")
	require.Equal(t, map[string]string{"vram": "40G"}, out["gpu1"], "a device whose own facts came through is unaffected")

	_, hasGPU0Key := out["gpu0"]
	require.False(t, hasGPU0Key,
		"a device that came back empty during a pass with a failure must be OMITTED (not sent as {}), or the server still clears it")
}

// The complement: when a device comes back empty and NOTHING failed
// anywhere this pass, that emptiness is trustworthy and is sent as an
// explicit, present clear — exactly Task 3's intended "a probe that stops
// reporting a key means the fact is gone" behavior.
func TestLabelsPayloadSendsExplicitClearWhenNothingFailed(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{"cpus": "8"},
		Device: map[string]map[string]string{"gpu0": {}},
	}
	out := labelsPayload(res, map[string]bool{})
	require.NotNil(t, out)
	gpu0, hasGPU0Key := out["gpu0"]
	require.True(t, hasGPU0Key, "a confirmed-clean empty device must still be a present key")
	require.Empty(t, gpu0)
}

// TestLabelsPayloadOmitsOnlyTheUnconfirmedOfTwoEmptyDevices is the Stage 4
// narrowing at this layer: two devices, both with no facts of their own,
// only ONE of them behind a failed source. The unconfirmed one is preserved
// by omission; the other is a present key, which is the only way the
// controller ever applies the "" key's host facts to it (see
// applyDeviceFacts server-side). Under the pass-wide flag both were omitted,
// and the second device's disk_free_bytes/mem_total_bytes froze along with
// facts it had nothing to do with.
func TestLabelsPayloadOmitsOnlyTheUnconfirmedOfTwoEmptyDevices(t *testing.T) {
	res := ProbeResult{
		Host:        map[string]string{"disk_free_bytes": "512"},
		Device:      map[string]map[string]string{"gpu0": {}, "gpu1": {}},
		Unconfirmed: map[string]bool{"gpu1": true},
	}
	out := labelsPayload(res, map[string]bool{})
	require.NotNil(t, out)
	require.Equal(t, map[string]string{"disk_free_bytes": "512"}, out[""])

	gpu0, hasGPU0Key := out["gpu0"]
	require.True(t, hasGPU0Key, "a device no failure speaks for must stay a present key, or it never sees the fresh host facts")
	require.Empty(t, gpu0)

	_, hasGPU1Key := out["gpu1"]
	require.False(t, hasGPU1Key, "the device whose own source failed is still preserved by omission")
}

// A pass with no host facts but SOME device fact is not "nothing": the ""
// key is simply absent (there is nothing host-wide to say), not present as
// an empty map.
func TestLabelsPayloadOmitsHostKeyWhenOnlyDeviceFactsExist(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{},
		Device: map[string]map[string]string{"gpu0": {"vram": "80G"}},
	}
	out := labelsPayload(res, map[string]bool{})
	require.NotNil(t, out)
	_, hasHostKey := out[""]
	require.False(t, hasHostKey)
	require.Equal(t, "80G", out["gpu0"]["vram"])
}

// TestLabelsPayloadWarnsOnceThenStaysQuietForTheSameEpisode is the
// fix-round-3 "reconsider the warning's placement" fix: an ongoing failure
// (the SAME device omitted pass after pass) must log the preserve warning
// once, not on every single pass — a warning that repeats forever trains an
// operator to ignore it, exactly the alert-fatigue concern that made
// nvidiaLabels' own "not found" line Debug-level rather than Warn in fix
// round 2.
func TestLabelsPayloadWarnsOnceThenStaysQuietForTheSameEpisode(t *testing.T) {
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	res := ProbeResult{
		Host:        map[string]string{"cpus": "8"},
		Device:      map[string]map[string]string{"gpu0": {}},
		Unconfirmed: map[string]bool{"gpu0": true},
	}
	warned := map[string]bool{}

	out1 := labelsPayload(res, warned)
	_, hasKey := out1["gpu0"]
	require.False(t, hasKey, "sanity: gpu0 omitted as expected")
	require.True(t, warned["gpu0"], "the device must be marked warned after the first omission")
	firstCount := countOccurrences(buf.String(), "gpu0")
	require.Equal(t, 1, firstCount, "exactly one log line naming gpu0 after the first omission")

	// Same failure, same device, three more passes: no additional lines.
	for i := 0; i < 3; i++ {
		labelsPayload(res, warned)
	}
	require.Equal(t, firstCount, countOccurrences(buf.String(), "gpu0"),
		"an ongoing failure must not re-log the same warning on every pass")
}

// TestLabelsPayloadWarnsAgainAfterRecovery is the complement: once a device
// recovers (its probe reports real facts again, or is confirmed cleanly
// empty), the warned marker resets — so a LATER, new failure episode logs
// again rather than staying silent forever because of a stale marker from a
// past, already-resolved episode.
func TestLabelsPayloadWarnsAgainAfterRecovery(t *testing.T) {
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	warned := map[string]bool{}
	failing := ProbeResult{
		Host:        map[string]string{"cpus": "8"},
		Device:      map[string]map[string]string{"gpu0": {}},
		Unconfirmed: map[string]bool{"gpu0": true},
	}
	labelsPayload(failing, warned)
	require.Equal(t, 1, countOccurrences(buf.String(), "gpu0"))

	// gpu0 recovers with real facts.
	recovered := ProbeResult{
		Host:   map[string]string{"cpus": "8"},
		Device: map[string]map[string]string{"gpu0": {"vram": "80G"}},
	}
	out := labelsPayload(recovered, warned)
	require.Equal(t, "80G", out["gpu0"]["vram"])
	require.False(t, warned["gpu0"], "recovery must clear the warned marker")

	// A NEW failure episode must log again, not stay silent.
	labelsPayload(failing, warned)
	require.Equal(t, 2, countOccurrences(buf.String(), "gpu0"),
		"a new failure episode after a recovery must log its own warning")
}

func countOccurrences(s, substr string) int {
	n := 0
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			n++
		}
	}
	return n
}

// declaredLabelsPayload always sends the config's declared labels verbatim,
// one entry per device (even an empty one) — there is no probe to fail here,
// worker.yaml is the operator's own declaration of intent, so an operator
// who deletes a device's labels and restarts must see them actually
// cleared, not preserved by some defensive skip.
func TestDeclaredLabelsPayloadReflectsConfigForEveryDevice(t *testing.T) {
	devices := []DeviceConfig{
		{Name: "gpu0", Labels: map[string]string{"owner": "team-a"}},
		{Name: "gpu1"}, // no declared labels at all
	}
	out := declaredLabelsPayload(devices)
	require.Equal(t, map[string]string{"owner": "team-a"}, out["gpu0"])
	require.Empty(t, out["gpu1"])
	_, ok := out["gpu1"]
	require.True(t, ok, "every configured device gets an entry, even an empty one")
}
