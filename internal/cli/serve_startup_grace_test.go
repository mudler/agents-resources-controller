package cli

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/notify"
	"github.com/stretchr/testify/require"
)

// The controller-restart defect of 2026-08-18, at the level that actually
// wires it: store.Sweep can hold off perfectly and still be handed a
// hold-off instant of zero by the loop that calls it, which is the whole
// mistake being fixed reappearing one layer up.

// A lease whose deadline lapsed while the controller was down must not be
// expired by the first sweep after it comes back, and — since a device
// silently leaving the pool is the one thing this event stream must never
// omit — nothing at all is announced about it.
func TestSweepAndNotifyHoldsOffInsideTheStartupGrace(t *testing.T) {
	st, c := newSweepStore(t)
	job := allocateJob(t, st)
	n, flush := newRecordingNotifier(t)

	// The controller was down for two minutes; the lease's minute-long TTL
	// lapsed while nobody was listening.
	c.Advance(2 * time.Minute)

	res, err := sweepAndNotify(st, n, HeartbeatGrace, 5*time.Minute, c.Now().Add(startupGrace))
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired, "the holder has not had a chance to renew yet")

	devices, err := st.Devices()
	require.NoError(t, err)
	require.NotEqual(t, model.DeviceUnhealthy, devices[0].State)

	reloaded, err := st.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobAssigned, reloaded.State, "the job was running fine")

	require.Empty(t, flush(), "nothing happened, so nothing is announced")
}

// And once the window has closed, the same sweep reaches the same verdict it
// always did. The grace delays judgement; it does not forgive.
func TestSweepAndNotifyStillExpiresALeaseOnceTheStartupGraceCloses(t *testing.T) {
	st, c := newSweepStore(t)
	job := allocateJob(t, st)
	n, flush := newRecordingNotifier(t)

	holdOffUntil := c.Now().Add(startupGrace)
	c.Advance(2 * time.Minute)

	res, err := sweepAndNotify(st, n, HeartbeatGrace, 5*time.Minute, holdOffUntil)
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)

	require.Equal(t, []notify.Event{
		{Kind: notify.KindLeaseExpired, Device: "gpubox:gpu0", Job: job.ID, Reason: "lease expired"},
		{Kind: notify.KindDeviceUnhealthy, Device: "gpubox:gpu0", Reason: "lease_expired"},
	}, flush())
}

// The loop, not the function. TestReaperAnnouncesALostWorkerThroughTheConfiguredWebhook
// proves the reaper's goroutine writes a silent worker off; it shortens the
// startup grace to get there, so on its own it would be equally happy with a
// controller that never had one. This is the other half: with the shipped
// grace in force, a real `rc serve` — real HTTP, real store, real webhook —
// must write NOTHING off during the window, however long the worker has been
// silent. Set against a one-millisecond unhealthyAfter, every sweep in this
// test would otherwise condemn the device immediately.
func TestTheReaperWritesNothingOffInsideTheStartupGrace(t *testing.T) {
	delivered := make(chan notify.Event, 16)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e notify.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err == nil {
			select {
			case delivered <- e:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(hook.Close)

	shorten(t, &sweepInterval, 20*time.Millisecond)
	shorten(t, &sweepUnhealthyAfter, time.Millisecond)
	// startupGrace is left at its shipped value: it is the thing under test.

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	t.Setenv("RC_TOKENS", "wtok:worker,ctok:client")
	cmd := NewServeCmd()
	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--addr", addr, "--data", t.TempDir(), "--webhook-url", hook.URL})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("rc serve did not shut down after its context was cancelled")
		}
	})

	// Registers once and never heartbeats again — a host that fell off the
	// network, or one the controller simply has not heard from since it
	// started.
	require.Eventually(t, func() bool {
		return registerWorkerRaw(t, "http://"+addr, "gracebox") == nil
	}, 5*time.Second, 50*time.Millisecond, "rc serve never came up")

	// Many sweeps' worth of wall clock, all of it inside the 30s window.
	select {
	case e := <-delivered:
		t.Fatalf("the reaper condemned %s (%s) inside the startup grace; "+
			"the window is not reaching the sweep", e.Device, e.Kind)
	case <-time.After(500 * time.Millisecond):
	}
}

// The window an operator actually gets, asserted where every other test
// shortens it away. Its value is not arbitrary: it is HeartbeatGrace, the
// controller's own definition of how long a worker may be quiet before it is
// treated as out of contact, so the sweep cannot expire a lease inside a
// window in which it does not yet consider the holder out of contact.
func TestStartupGraceIsHeartbeatGrace(t *testing.T) {
	require.Equal(t, HeartbeatGrace, startupGrace)
	require.Equal(t, 30*time.Second, startupGrace)
}
