package server_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// TestDescribeReturnsLabelsWithBothSourcesAndTimestamps is the core promise
// of `rc describe`: an agent about to write commands for a box needs to see
// BOTH the detected and declared value for a key, each with its own
// provenance and the real moment it was last confirmed — not "now", which
// would defeat the entire point of showing an age at all.
func TestDescribeReturnsLabelsWithBothSourcesAndTimestamps(t *testing.T) {
	ts, _, _, c := newServer(t)

	stamped := c.Now()
	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels:         map[string]map[string]string{"gpu0": {"vram": "80G"}},
		DeclaredLabels: map[string]map[string]string{"gpu0": {"vram": "40G"}},
	})
	reg.Body.Close()
	require.Equal(t, http.StatusOK, reg.StatusCode)

	// Time moves on after the labels were recorded — the response must still
	// carry the moment they were actually stored, not the request time.
	c.Advance(10 * time.Minute)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Equal(t, "gpubox:gpu0", out.Device.ID)

	var detected, declared *model.Label
	for i := range out.Labels {
		l := &out.Labels[i]
		if l.Key != "vram" {
			continue
		}
		switch l.Source {
		case model.SourceDetected:
			detected = l
		case model.SourceDeclared:
			declared = l
		}
	}
	require.NotNil(t, detected, "expected a detected vram label")
	require.NotNil(t, declared, "expected a declared vram label conflicting with it")
	require.Equal(t, "80G", detected.Value)
	require.Equal(t, "40G", declared.Value)
	require.WithinDuration(t, stamped, detected.UpdatedAt, time.Second,
		"the label's age must be measured from when it was actually recorded, not from the describe request")
	require.WithinDuration(t, stamped, declared.UpdatedAt, time.Second)
}

// TestDescribeReturnsSheetAndAge pins the other half of "age is not
// decoration": a usage sheet last edited long ago must report that real
// timestamp so a stale sheet is visibly stale, not silently treated as
// current documentation.
func TestDescribeReturnsSheetAndAge(t *testing.T) {
	ts, _, _, c := newServer(t)

	stamped := c.Now()
	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		DeviceSheets: map[string]string{"gpu0": "gpu0 runs the nightly eval suite"},
	})
	reg.Body.Close()
	require.Equal(t, http.StatusOK, reg.StatusCode)

	c.Advance(90 * 24 * time.Hour) // "last edited in March" territory

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Equal(t, "gpu0 runs the nightly eval suite", out.Sheet)
	require.WithinDuration(t, stamped, out.SheetUpdatedAt, time.Second,
		"the sheet's age must reflect when it was actually written, however long ago")
	require.False(t, out.SheetIsHostWide, "a device's own sheet must not be reported as the host-wide fallback")
}

// TestDescribeSheetFallsBackToHostWideWhenNoDeviceSheet: a device with no
// sheet of its own still surfaces whatever the humans wrote for the whole
// host, rather than describe going silent about documentation that exists.
func TestDescribeSheetFallsBackToHostWideWhenNoDeviceSheet(t *testing.T) {
	ts, _, _, _ := newServer(t)

	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Sheet: ptr("# gpubox\nshared rack A1, ask #infra before touching cabling"),
	})
	reg.Body.Close()
	require.Equal(t, http.StatusOK, reg.StatusCode)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Contains(t, out.Sheet, "shared rack A1")
	require.True(t, out.SheetIsHostWide, "the fallback sheet must be flagged as host-wide, not attributed to this device")
}

// TestDescribeIncludesHolderAndRecentJobs proves the device-status and
// job-history halves of the response actually carry real data end to end —
// not just an empty slice a looser test wouldn't have caught missing.
func TestDescribeIncludesHolderAndRecentJobs(t *testing.T) {
	ts, st, _, c := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	var firstJob model.Job
	require.NoError(t, json.NewDecoder(first.Body).Decode(&firstJob))
	first.Body.Close()

	code := 0
	require.NoError(t, st.Release(firstJob.ID, model.JobSucceeded, &code, ""))
	c.Advance(time.Minute)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./train"}, Submitter: "agent-b",
	})
	var secondJob model.Job
	require.NoError(t, json.NewDecoder(second.Body).Decode(&secondJob))
	second.Body.Close()

	c.Advance(45 * time.Second)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Equal(t, "agent-b", out.Holder, "the device is currently held by the second job's submitter")
	require.Equal(t, secondJob.ID, out.JobID)
	require.Equal(t, 45, out.ElapsedSeconds)

	require.Len(t, out.RecentJobs, 2)
	require.Equal(t, secondJob.ID, out.RecentJobs[0].ID, "most recent job must come first")
	require.Equal(t, firstJob.ID, out.RecentJobs[1].ID)
}

func TestDescribeUnknownDeviceIs404(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:no-such-gpu/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDescribeRequiresClientToken(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	unauth := get(t, ts, "bogus", "/v1/devices/gpubox:gpu0/describe")
	defer unauth.Body.Close()
	require.Equal(t, http.StatusUnauthorized, unauth.StatusCode)

	// A worker token is a real, known token — just the wrong role for a
	// client-facing route.
	wrongRole := get(t, ts, "wtok", "/v1/devices/gpubox:gpu0/describe")
	defer wrongRole.Body.Close()
	require.Equal(t, http.StatusForbidden, wrongRole.StatusCode)
}

// TestExplainReturnsMatchingFreeDevicesAndQueueDepth is the guard against a
// smoke test that merely checks the route returns 200: it sets up one busy
// device and one free device that both match the selector, plus a job
// queued behind the busy one, and checks every field the design promises —
// "which devices match, how many are free, and the queue depth" — actually
// reflects that scenario rather than a vacuous zero value.
func TestExplainReturnsMatchingFreeDevicesAndQueueDepth(t *testing.T) {
	ts, st, _, _ := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}, {Name: "gpu1"}},
		Labels: map[string]map[string]string{
			"gpu0": {"vram": "80G"},
			"gpu1": {"vram": "48G"},
		},
	}).Body.Close()

	// gpu0 is taken; a second job for it queues behind.
	busy := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	busy.Body.Close()
	post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench2"}, Submitter: "agent-b",
	}).Body.Close()

	resp := get(t, ts, "ctok", "/v1/explain?selector=vram%3E%3D40G")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.ExplainResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.ElementsMatch(t, []string{"gpubox:gpu0", "gpubox:gpu1"}, out.Matching)
	require.ElementsMatch(t, []string{"gpubox:gpu1"}, out.Free, "gpu0 is busy and must not be reported free")
	require.Equal(t, 1, out.QueueDepth, "one job is queued behind gpu0, which matches the selector")

	// Sanity: the labels really did land, or this test would pass for the
	// wrong reason (an empty selector matching nothing produces empty
	// Matching/Free too).
	labels, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.NotEmpty(t, labels)
}

func TestExplainExcludesNonMatchingDevices(t *testing.T) {
	ts, _, _, _ := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}, {Name: "gpu1"}},
		Labels: map[string]map[string]string{
			"gpu0": {"vram": "80G"},
			"gpu1": {"vram": "8G"},
		},
	}).Body.Close()

	resp := get(t, ts, "ctok", "/v1/explain?selector=vram%3E%3D40G")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.ExplainResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, []string{"gpubox:gpu0"}, out.Matching)
	require.NotContains(t, out.Matching, "gpubox:gpu1")
}

func TestExplainRequiresSelectorParam(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp := get(t, ts, "ctok", "/v1/explain")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestExplainRejectsMalformedSelector(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp := get(t, ts, "ctok", "/v1/explain?selector=not-a-valid-term")
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestExplainRequiresClientToken(t *testing.T) {
	ts, _, _, _ := newServer(t)

	unauth := get(t, ts, "bogus", "/v1/explain?selector=vram%3E%3D40G")
	defer unauth.Body.Close()
	require.Equal(t, http.StatusUnauthorized, unauth.StatusCode)

	wrongRole := get(t, ts, "wtok", "/v1/explain?selector=vram%3E%3D40G")
	defer wrongRole.Body.Close()
	require.Equal(t, http.StatusForbidden, wrongRole.StatusCode)
}
