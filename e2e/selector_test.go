// Package e2e_test's selector tests cover what stage 3 added: a job can
// target a device by its labels instead of a device ID, the match is
// resolved against real labels sitting in the real store behind newFleet's
// real controller, and a selector that names no device at all is refused
// outright rather than queued forever.
package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// TestSelectorLandsOnlyOnTheMatchingDevice stands up newFleet's one real
// device, gives it detected labels distinguishing it from a second device,
// and proves a --select job lands on the one that matches and never touches
// the one that doesn't.
//
// The second device is not a second real worker process: it is registered
// straight through the store newFleet already returns (store.UpsertWorker,
// the exact call handleRegister makes on the wire), which is the harness's
// own escape hatch for "one more device with different facts" rather than
// standing up a second worker.New/httptest pair — see the task 9 brief's
// instruction to reuse newFleet rather than build a second harness. Nothing
// ever polls for that second device's jobs, which is fine: the whole point
// is that the selector must never send it any.
func TestSelectorLandsOnlyOnTheMatchingDevice(t *testing.T) {
	cl, st, device := newFleet(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	now := st.Now()

	// The real device (newFleet's worker already registered it with empty
	// detected labels — nothing device-scoped for a real host's built-in
	// probes to find). ReplaceLabels overwrites that empty set with facts a
	// GPU probe would actually have reported.
	require.NoError(t, st.ReplaceLabels(device, model.SourceDetected,
		map[string]string{"vendor": "nvidia", "vram": "80G"}, now))

	// A second device on a different, entirely fictitious host — never
	// backed by a real worker process — carrying a DIFFERENT vendor so the
	// selector below must reject it.
	const otherHost = "otherbox"
	const otherDevice = otherHost + ":gpu0"
	require.NoError(t, st.UpsertWorker(
		model.Worker{ID: otherHost, Host: otherHost, BootID: "boot-other", LastHeartbeatAt: now},
		[]model.Device{{ID: otherDevice, Host: otherHost, Name: "gpu0", WorkerID: otherHost, State: model.DeviceReady}},
	))
	require.NoError(t, st.ReplaceLabels(otherDevice, model.SourceDetected,
		map[string]string{"vendor": "amd", "vram": "80G"}, now))

	// Sanity: both devices exist and are visible before the real assertion,
	// so a failure below is about selector matching, not about the fixture
	// itself never having registered.
	state, err := cl.State(ctx)
	require.NoError(t, err)
	require.Len(t, state.Devices, 2, "expected the real device plus the fixture device")

	job, err := cl.Submit(ctx, client.SubmitOptions{
		Selector:  "vendor=nvidia,vram>=40G",
		Command:   []string{"sh", "-c", "echo picked-the-right-device"},
		Submitter: "agent-a",
	})
	require.NoError(t, err, "a selector matching exactly one free device must be accepted")
	require.Equal(t, device, job.DeviceID,
		"the selector must resolve to the nvidia device, not the amd one")

	final, err := cl.WaitTerminal(ctx, job.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final.State)

	var out logBuffer
	require.NoError(t, cl.StreamLogs(ctx, job.ID, &out))
	require.Contains(t, out.String(), "picked-the-right-device")

	// The non-matching device must never have been touched: no lease taken,
	// still ready, never even considered a candidate. This is checked AFTER
	// the matching job has already run to completion, so the scheduler has
	// had every chance it will ever get to make a mistake here.
	state, err = cl.State(ctx)
	require.NoError(t, err)
	for _, v := range state.Devices {
		if v.Device.ID != otherDevice {
			continue
		}
		require.Equal(t, model.DeviceReady, v.Device.State,
			"the non-matching device must never change state because of a selector job it doesn't match")
		require.Empty(t, v.Holder, "the non-matching device must never be handed a lease")
	}
}

// TestSelectorMatchingNoDeviceIsRejectedAtSubmit proves the deliberate stage
// 3 choice documented in the README and the task brief: a selector that
// matches nothing right now is refused immediately, with a clear reason —
// not accepted and left to queue forever on the hope some future device
// registers with the right labels. A typo in --select is far more common
// than that bet paying off, and an indefinitely queued job is
// indistinguishable from a hang.
func TestSelectorMatchingNoDeviceIsRejectedAtSubmit(t *testing.T) {
	// The device's distinguishing fact arrives through registration
	// (worker.yaml's declared labels), not through a store write behind the
	// controller's back. That matters here more than anywhere else in this
	// file: a fixture whose label never landed would make this test pass for
	// the wrong reason entirely — a selector matching nothing on a device
	// with NO labels at all, rather than one whose labels genuinely disagree.
	// The positive control at the end is the other half of that guard.
	cl, _, device := newFleet(t, 0, withLabels(map[string]string{"vendor": "nvidia"}))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := cl.Submit(ctx, client.SubmitOptions{
		Selector:  "vendor=intel",
		Command:   []string{"true"},
		Submitter: "agent-a",
	})
	require.Error(t, err, "a selector matching no device must be rejected, not queued")
	require.ErrorContains(t, err, "no_matching_device")
	require.ErrorContains(t, err, "vendor=intel")

	// And it really was rejected outright, not silently queued: nothing
	// should show up waiting for a device that doesn't exist.
	state, err := cl.State(ctx)
	require.NoError(t, err)
	require.Empty(t, state.Queued, "a rejected selector submit must never leave a job queued behind it")

	// The positive control: the same device, selected on the label it
	// actually carries, IS accepted and runs. Without this, every assertion
	// above would still hold on a device carrying no labels whatsoever —
	// which is a fixture failure wearing a passing test's clothes.
	matched, err := cl.Submit(ctx, client.SubmitOptions{
		Selector:  "vendor=nvidia",
		Command:   []string{"true"},
		Submitter: "agent-a",
	})
	require.NoError(t, err, "the device's declared vendor label never reached the controller")
	require.Equal(t, device, matched.DeviceID)

	final, err := cl.WaitTerminal(ctx, matched.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final.State)
}
