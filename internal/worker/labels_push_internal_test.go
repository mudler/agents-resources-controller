package worker

import (
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
// everyday guard is per-device, see the Failed-flag tests below — a real
// gatherLabels pass almost always has non-empty Host (builtinLabels always
// yields at least "cpus"), which is exactly why a pass-wide-only guard was
// found inert in fix round 1: see
// TestLabelsPayloadOmitsFailedDeviceButKeepsHostAndOtherDevices.
func TestLabelsPayloadNilWhenPassProducedNothing(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{},
		Device: map[string]map[string]string{"gpu0": {}, "gpu1": {}},
	}
	require.Nil(t, labelsPayload(res), "a pass with zero facts anywhere must send nil, not an empty map")
}

// When nothing failed this pass, an empty device result is trustworthy: the
// full snapshot is sent, including devices that themselves got nothing this
// round (an entirely ordinary, legitimate report — that specific device's
// own facts didn't change).
func TestLabelsPayloadIncludesEverythingWhenPassProducedAnything(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{"kernel": "6.8.0"},
		Device: map[string]map[string]string{"gpu0": {"vram": "80G"}, "gpu1": {}},
		Failed: false,
	}
	out := labelsPayload(res)
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
// pass (its probe failed — res.Failed is true), so gpu0 must be OMITTED
// from the output entirely — not sent as {}, which ReplaceLabels would
// still turn into a clear — while gpu1 (whose own facts came through fine)
// and the host-wide key are unaffected.
func TestLabelsPayloadOmitsFailedDeviceButKeepsHostAndOtherDevices(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{"cpus": "8"},
		Device: map[string]map[string]string{"gpu0": {}, "gpu1": {"vram": "40G"}},
		Failed: true,
	}
	out := labelsPayload(res)
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
		Failed: false,
	}
	out := labelsPayload(res)
	require.NotNil(t, out)
	gpu0, hasGPU0Key := out["gpu0"]
	require.True(t, hasGPU0Key, "a confirmed-clean empty device must still be a present key")
	require.Empty(t, gpu0)
}

// A pass with no host facts but SOME device fact is not "nothing": the ""
// key is simply absent (there is nothing host-wide to say), not present as
// an empty map.
func TestLabelsPayloadOmitsHostKeyWhenOnlyDeviceFactsExist(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{},
		Device: map[string]map[string]string{"gpu0": {"vram": "80G"}},
	}
	out := labelsPayload(res)
	require.NotNil(t, out)
	_, hasHostKey := out[""]
	require.False(t, hasHostKey)
	require.Equal(t, "80G", out["gpu0"]["vram"])
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
