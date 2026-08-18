package worker

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// fixedJitter is a tracker whose jitter is a known constant, so a test can
// assert on the exact instant a re-registration becomes due rather than on a
// range.
func fixedJitter(d time.Duration) *contactTracker {
	t := newContactTracker()
	t.jitter = func() time.Duration { return d }
	return t
}

var base = time.Date(2026, 8, 18, 18, 39, 0, 0, time.UTC)

// A worker's very first contact is not a reconnection. Arming on it would
// send every worker in the fleet through registration twice at startup, for
// nothing.
func TestTheFirstContactOwesNoReRegistration(t *testing.T) {
	tr := fixedJitter(0)
	tr.observe(base, "controller-a")
	require.False(t, tr.due(base))
	require.False(t, tr.due(base.Add(time.Hour)))
}

// The blip guard. A single failed heartbeat, a dropped long poll, a proxy
// hiccup: the gap is short, the controller is the same one, and nothing about
// this worker's registration has gone stale. Re-registering here would be a
// fleet-wide storm triggered by one packet loss — and registration is not
// free, it reaps whatever the controller thinks is in flight.
func TestABriefGapInContactOwesNoReRegistration(t *testing.T) {
	tr := fixedJitter(0)
	tr.observe(base, "controller-a")

	// Several heartbeats' worth of silence, well short of the horizon past
	// which the controller writes a silent worker's devices off.
	tr.observe(base.Add(90*time.Second), "controller-a")
	require.False(t, tr.due(base.Add(90*time.Second)))
}

// Silence long enough for the controller to have written this worker's
// devices off is the case registration exists to repair: only registration
// carries the proof (a boot ID, a survivor scan) that can bring them back.
func TestSilencePastTheWriteOffHorizonOwesAReRegistration(t *testing.T) {
	tr := fixedJitter(5 * time.Second)
	tr.observe(base, "controller-a")

	back := base.Add(reregisterAfterSilence)
	tr.observe(back, "controller-a")

	require.False(t, tr.due(back), "the jitter delay must be waited out first")
	require.False(t, tr.due(back.Add(5*time.Second-time.Nanosecond)))
	require.True(t, tr.due(back.Add(5*time.Second)))
}

// The controller-restart case, and the reason a failure streak is not enough
// on its own: a restart short enough not to fail a single request is
// invisible except through the identity the controller reports. That is
// exactly the restart of 2026-08-18 — down for a few seconds, and a device
// left quarantined with nobody left to prove it clean.
func TestAChangedControllerInstanceOwesAReRegistration(t *testing.T) {
	tr := fixedJitter(0)
	tr.observe(base, "controller-a")
	require.False(t, tr.due(base))

	tr.observe(base.Add(10*time.Second), "controller-b")
	require.True(t, tr.due(base.Add(10*time.Second)))
}

// A controller that predates this header (or a proxy that strips it) reports
// nothing, and nothing must never read as "a different controller" — that
// would put every worker into a re-registration loop against an older
// controller, which is the worst possible response to talking to one.
func TestAnUnnamedControllerOwesNoReRegistration(t *testing.T) {
	tr := fixedJitter(0)
	tr.observe(base, "controller-a")
	tr.observe(base.Add(10*time.Second), "")
	require.False(t, tr.due(base.Add(10*time.Second)))

	// And the identity it did report is not forgotten by the silent
	// response: the same controller answering again is still the same
	// controller.
	tr.observe(base.Add(20*time.Second), "controller-a")
	require.False(t, tr.due(base.Add(20*time.Second)))
}

// Once the registration has actually happened the debt is settled. Without
// this the poll loop would re-register on every pass for as long as the
// worker stayed idle.
func TestASettledReRegistrationIsNotOwedAgain(t *testing.T) {
	tr := fixedJitter(0)
	tr.observe(base, "controller-a")
	tr.observe(base.Add(10*time.Second), "controller-b")
	require.True(t, tr.due(base.Add(10*time.Second)))

	tr.attempting(base.Add(10 * time.Second))
	tr.registered()

	require.False(t, tr.due(base.Add(10*time.Second)))
	tr.observe(base.Add(20*time.Second), "controller-b")
	require.False(t, tr.due(base.Add(time.Hour)))
}

// A re-registration that failed is still owed — the device it was going to
// prove clean is still out of the pool — but it is not retried at the speed
// of the poll loop, which would be several attempts a second against a
// controller that is evidently already unwell.
func TestAFailedAttemptIsStillOwedButPaced(t *testing.T) {
	tr := fixedJitter(0)
	tr.observe(base, "controller-a")
	tr.observe(base.Add(10*time.Second), "controller-b")

	at := base.Add(10 * time.Second)
	tr.attempting(at)
	// registered() is never called: the attempt failed.
	require.False(t, tr.due(at), "an attempt just made must not be repeated immediately")
	require.False(t, tr.due(at.Add(reregisterRetryAfter-time.Second)))
	require.True(t, tr.due(at.Add(reregisterRetryAfter)))
}

// The anti-storm property, at the level it actually acts: when a controller
// comes back, every worker in the fleet is armed within milliseconds of every
// other one, and they must not all register at the same instant. Each draws
// its own delay from the whole window.
func TestTheDefaultJitterSpreadsAFleetAcrossTheWindow(t *testing.T) {
	tr := newContactTracker()
	require.NotNil(t, tr.jitter, "a tracker with no jitter is a synchronised fleet")

	seen := map[time.Duration]bool{}
	for range 100 {
		d := tr.jitter()
		require.GreaterOrEqual(t, d, time.Duration(0))
		require.LessOrEqual(t, d, reregisterJitter)
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "every worker drawing the same delay is not a spread")
}

// The horizon is the controller's own: sweepUnhealthyAfter in
// internal/cli/serve.go, which cannot be imported here (that package builds
// the worker command). A worker that re-registered on a shorter silence
// would be repairing damage the controller had not done.
func TestTheReconnectionThresholdsAreTheShippedOnes(t *testing.T) {
	require.Equal(t, 5*time.Minute, reregisterAfterSilence)
	require.Equal(t, 30*time.Second, reregisterJitter)
	require.Equal(t, time.Minute, reregisterRetryAfter)
}

// The header is mirrored, not imported, so that nothing in the worker depends
// on the controller's implementation — the same rule registerRequest follows.
// A mirror needs a mirror test: a header the controller stamps and the worker
// never reads is a reconnection that silently stops happening.
func TestTheMirroredInstanceHeaderIsTheOneTheControllerSends(t *testing.T) {
	require.Equal(t, server.ControllerInstanceHeader, controllerInstanceHeader)
}
