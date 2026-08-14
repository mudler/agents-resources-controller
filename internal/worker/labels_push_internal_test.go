package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// This is the trap carried forward from Task 3: ReplaceLabels treats an
// empty map as "clear this source", and the controller cannot tell "the
// probe ran and found nothing" from "the probe did not run at all". A whole
// pass that produced nothing must translate to nil here, not {}, or a
// fleet-wide probe outage would wipe every device's detected labels on the
// next push.
func TestLabelsPayloadNilWhenPassProducedNothing(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{},
		Device: map[string]map[string]string{"gpu0": {}, "gpu1": {}},
	}
	require.Nil(t, labelsPayload(res), "a pass with zero facts anywhere must send nil, not an empty map")
}

// The instant ANY fact was gathered anywhere — host or a single device —
// the pass is not "failed", so the full snapshot is sent, including devices
// that themselves got nothing this round (which is an entirely ordinary,
// legitimate report: that specific device's own facts didn't change).
func TestLabelsPayloadIncludesEverythingWhenPassProducedAnything(t *testing.T) {
	res := ProbeResult{
		Host:   map[string]string{"kernel": "6.8.0"},
		Device: map[string]map[string]string{"gpu0": {"vram": "80G"}, "gpu1": {}},
	}
	out := labelsPayload(res)
	require.NotNil(t, out)
	require.Equal(t, map[string]string{"kernel": "6.8.0"}, out[""])
	require.Equal(t, map[string]string{"vram": "80G"}, out["gpu0"])
	require.Empty(t, out["gpu1"], "gpu1's own empty result is sent as-is, not treated as pass failure")
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
