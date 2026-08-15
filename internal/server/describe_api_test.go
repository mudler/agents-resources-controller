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

// TestDescribeComputesLabelAgeFromControllerClock is the server-side half
// of the clock-skew fix: it advances the FAKE clock (never sleeps) a known
// amount past a label's timestamp and asserts LabelAgeSeconds carries
// exactly that value, keyed by key+"/"+source, for both a detected and a
// declared label recorded at different moments. Getting this from
// UpdatedAt/time.Now() on the reader's side is exactly the bug this field
// exists to remove — this test pins the number the CONTROLLER computes,
// independent of whatever clock a CLI happened to read it with.
func TestDescribeComputesLabelAgeFromControllerClock(t *testing.T) {
	ts, _, _, c := newServer(t)

	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels: map[string]map[string]string{"gpu0": {"vram": "80G"}},
	})
	var regResp server.RegisterResponse
	require.NoError(t, json.NewDecoder(reg.Body).Decode(&regResp))
	reg.Body.Close()

	c.Advance(10 * time.Minute)

	post(t, ts, "wtok", "/v1/workers/"+regResp.WorkerID+"/labels", server.LabelsPushRequest{
		Host: "gpubox", Devices: []string{"gpu0"},
		DeclaredLabels: map[string]map[string]string{"gpu0": {"vram": "40G"}},
	}).Body.Close()

	// detected/vram is now 25 minutes old, declared/vram is 15 minutes old:
	// two different, known ages recorded at two different moments.
	c.Advance(15 * time.Minute)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Equal(t, 25*60, out.LabelAgeSeconds["vram/"+model.SourceDetected],
		"the detected label's age must be the controller clock's own elapsed time since it was recorded")
	require.Equal(t, 15*60, out.LabelAgeSeconds["vram/"+model.SourceDeclared],
		"the declared label's age must reflect its own, later, recording time - not be conflated with the detected one")
}

// TestDescribeFutureStampedLabelReportsNegativeAge pins the controller-side
// half of the future-stamped case: a label timestamped ahead of the
// controller's own clock (a worker with a fast clock) must not be silently
// floored to zero here. Flooring at the server would already have thrown
// away the anomaly before the CLI's rendering ever saw it, defeating
// formatAge's "in the future" warning regardless of how well the CLI
// renders it.
func TestDescribeFutureStampedLabelReportsNegativeAge(t *testing.T) {
	ts, st, _, c := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
	}).Body.Close()

	// Stamp the label 5 minutes AHEAD of the controller's current clock -
	// the shape a worker with a fast clock produces.
	future := c.Now().Add(5 * time.Minute)
	require.NoError(t, st.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vram": "80G"}, future))

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Equal(t, -5*60, out.LabelAgeSeconds["vram/"+model.SourceDetected],
		"a label stamped ahead of the controller's clock must report a negative age, not be floored to 0")
}

// TestDescribeFutureStampedSheetReportsNegativeAge is the sheet-side twin
// of TestDescribeFutureStampedLabelReportsNegativeAge: FIX 4 closed a real
// gap where clamping sheetAge at zero on the server survived the full test
// suite because nothing exercised a future-stamped SHEET at all, only a
// future-stamped label.
func TestDescribeFutureStampedSheetReportsNegativeAge(t *testing.T) {
	ts, st, _, c := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
	}).Body.Close()

	future := c.Now().Add(10 * time.Minute)
	require.NoError(t, st.UpsertHostDoc("gpubox", "gpubox:gpu0", "shared rack A1", future))

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.NotNil(t, out.SheetAgeSeconds)
	require.Equal(t, -10*60, *out.SheetAgeSeconds,
		"a sheet stamped ahead of the controller's clock must report a negative age, not be floored to 0")
}

// TestDescribeLabelAgeKeyDoesNotCollideOnSlashInLabelKey guards the
// key+"/"+source join DescribeResponse.LabelAgeSeconds uses: a label key
// comes from a worker's YAML config or a probe script, neither of which
// constrains its charset, so a key can itself contain "/". This registers
// one such key ("gpu/pci-slot") alongside a plain one ("vram"), each with
// both sources and a DIFFERENT, known age, and asserts all four ages land
// under their own distinct key rather than two of them overwriting each
// other. store.ReplaceLabels restricts source to exactly "detected" and
// "declared", NEITHER OF WHICH CONTAINS "/" — which is the necessary and
// sufficient condition that makes this join collision-free regardless of
// what the key contains (brute-forced over 599,186 pairs; see
// DescribeResponse.LabelAgeSeconds's doc comment for the full argument, and
// TestLabelSourcesNeverContainASlash below for the trip-wire that keeps a
// future source honoring it).
func TestDescribeLabelAgeKeyDoesNotCollideOnSlashInLabelKey(t *testing.T) {
	ts, _, _, c := newServer(t)

	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels:         map[string]map[string]string{"gpu0": {"vram": "80G", "gpu/pci-slot": "0000:01:00.0"}},
		DeclaredLabels: map[string]map[string]string{"gpu0": {"vram": "40G", "gpu/pci-slot": "slot-1"}},
	})
	var regResp server.RegisterResponse
	require.NoError(t, json.NewDecoder(reg.Body).Decode(&regResp))
	reg.Body.Close()

	c.Advance(20 * time.Minute)

	// Re-push only the DETECTED labels (DeclaredLabels omitted/nil, which
	// applyDeviceFacts treats as "no news about that source" and leaves
	// untouched — see its own doc comment) so detected gets a fresh,
	// later timestamp while declared keeps its original, older one.
	post(t, ts, "wtok", "/v1/workers/"+regResp.WorkerID+"/labels", server.LabelsPushRequest{
		Host: "gpubox", Devices: []string{"gpu0"},
		Labels: map[string]map[string]string{"gpu0": {"vram": "80G", "gpu/pci-slot": "0000:01:00.0"}},
	}).Body.Close()

	c.Advance(5 * time.Minute)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Len(t, out.LabelAgeSeconds, 4,
		"four distinct (key,source) pairs must produce four distinct map entries, none overwritten by another")
	require.Equal(t, 5*60, out.LabelAgeSeconds["vram/"+model.SourceDetected])
	require.Equal(t, 25*60, out.LabelAgeSeconds["vram/"+model.SourceDeclared])
	require.Equal(t, 5*60, out.LabelAgeSeconds["gpu/pci-slot/"+model.SourceDetected])
	require.Equal(t, 25*60, out.LabelAgeSeconds["gpu/pci-slot/"+model.SourceDeclared])
}

// TestLabelSourcesNeverContainASlash is the trip-wire for the
// key+"/"+source join's real invariant: NO label source literal may ever
// contain "/", regardless of length (equal length, which an earlier
// version of DescribeResponse.LabelAgeSeconds's own doc comment claimed
// was what mattered, is irrelevant to collision-safety — see that comment
// for the corrected argument). This test cannot see a THIRD source added
// in the future — there is nothing to assert against yet — so it exists
// purely as a trip-wire on TODAY's two: if either model.SourceDetected or
// model.SourceDeclared is ever changed to include a "/", this fails
// immediately, before anyone has to rediscover the collision by hand the
// way this task's review round did (brute force: a source shaped like
// "a/declared" produces 585 collisions against realistic keys).
func TestLabelSourcesNeverContainASlash(t *testing.T) {
	for _, src := range []string{model.SourceDetected, model.SourceDeclared} {
		require.NotContains(t, src, "/",
			"a label source containing '/' can make DescribeResponse.LabelAgeSeconds's key+\"/\"+source join collide, misattributing one label's age to another's key")
	}
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
	require.NotNil(t, out.SheetAgeSeconds, "a real, non-empty sheet must always carry a computed age")
	require.Equal(t, int(90*24*time.Hour/time.Second), *out.SheetAgeSeconds,
		"SheetAgeSeconds must be the controller clock's own elapsed time since the sheet was recorded")
	require.False(t, out.SheetIsHostWide, "a device's own sheet must not be reported as the host-wide fallback")
}

// TestDescribeSheetAgeSurvivesRequestTimeNotAffectingIt guards against the
// server computing SheetAgeSeconds from the HTTP request's own arrival time
// (e.g. an accidental time.Now() slipped back in): two describe requests
// issued back to back against the same fake-clock instant must report the
// identical age, not one that silently ticks with wall-clock time spent
// between them.
func TestDescribeSheetAgeSurvivesRequestTimeNotAffectingIt(t *testing.T) {
	ts, _, _, c := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		DeviceSheets: map[string]string{"gpu0": "note"},
	}).Body.Close()
	c.Advance(time.Hour)

	first := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	var out1 server.DescribeResponse
	require.NoError(t, json.NewDecoder(first.Body).Decode(&out1))
	first.Body.Close()

	second := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	var out2 server.DescribeResponse
	require.NoError(t, json.NewDecoder(second.Body).Decode(&out2))
	second.Body.Close()

	require.NotNil(t, out1.SheetAgeSeconds)
	require.NotNil(t, out2.SheetAgeSeconds)
	require.Equal(t, *out1.SheetAgeSeconds, *out2.SheetAgeSeconds,
		"the fake clock did not advance between requests, so the reported age must not have changed either")
	require.Equal(t, int(time.Hour/time.Second), *out1.SheetAgeSeconds)
}

// TestDescribeSheetAgeIsNilWhenNoSheetExists guards the one case
// SheetAgeSeconds cannot just be now.Sub(SheetUpdatedAt): when there is no
// sheet at all, SheetUpdatedAt is the zero time, and a naive subtraction
// against it would report several decades of "age" rather than the honest
// "nothing to report an age for" a caller with no sheet to distrust should
// see. This is the synthetic "nothing was EVER registered, not even an
// empty sheet" shape (a bare RegisterRequest with no Sheet field at all);
// see TestDescribeSheetAgeIsNilForRealisticEmptySheet below for the shape a
// real worker actually produces when it has no host.md.
func TestDescribeSheetAgeIsNilWhenNoSheetExists(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts) // no Sheet, no DeviceSheets

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Equal(t, "", out.Sheet)
	require.True(t, out.SheetUpdatedAt.IsZero())
	require.Nil(t, out.SheetAgeSeconds,
		"no sheet means nothing to report an age for, not decades of now.Sub(zero time)")
}

// TestDescribeSheetAgeIsNilForRealisticEmptySheet is FIX 6's server-side
// pin: the REALISTIC shape a worker with no host.md produces is NOT "empty
// body, zero timestamp" — internal/worker/worker.go's sheetPayload sends a
// non-nil pointer to "" in that case (readSheetFile treats a genuinely
// missing file as a successful empty read, not an error), so
// applyDeviceFacts stores a REAL row with a real, non-zero timestamp for
// the empty host-wide sheet. Gating SheetAgeSeconds on sheetAt.IsZero(), as
// an earlier version of handleDescribe did, would never protect against
// this — the single most common real-world "no sheet" shape — because that
// timestamp is never actually zero here. The gate must be on the body
// being empty, which it is regardless of the (very real) timestamp
// attached to recording that emptiness.
func TestDescribeSheetAgeIsNilForRealisticEmptySheet(t *testing.T) {
	ts, _, _, c := newServer(t)

	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		// A real worker with no host.md sends a non-nil pointer to "", not
		// an absent Sheet field. See sheetPayload's own doc comment.
		Sheet: ptr(""),
		// Likewise for a device with no host.d/gpu0.md of its own.
		DeviceSheets: map[string]string{"gpu0": ""},
	})
	reg.Body.Close()
	require.Equal(t, http.StatusOK, reg.StatusCode)

	c.Advance(time.Hour)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	require.Equal(t, "", out.Sheet)
	require.False(t, out.SheetUpdatedAt.IsZero(),
		"a real worker's empty-sheet registration leaves a real, non-zero timestamp - this is what makes the naive zero-timestamp gate wrong")
	require.Nil(t, out.SheetAgeSeconds,
		"an empty sheet has nothing to report an age for, regardless of the real timestamp attached to it")
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

// TestDescribeSheetFallsBackToHostWideEvenAfterAnExplicitEmptyDeviceSheet
// pins the exact shape a REAL worker registers in, not just the synthetic
// "DeviceSheets omitted entirely" shape the test above uses: worker.go's
// readSheets sends an explicit (if empty) entry in device_sheets for every
// device it declares, whether or not that device has its own host.d/<name>.md
// — a missing per-device file reads as "" the same way an empty one would.
// That means applyDeviceFacts stores a REAL host_docs row for gpu0 with an
// empty body and a real (non-zero) timestamp on this very first
// registration — the exact row shape that used to defeat the "empty body
// AND zero timestamp" fallback check in handleDescribe forever, because
// that row's timestamp is never zero from here on. A device that only ever
// gets this "I have nothing of my own" empty entry must still see the
// host-wide sheet.
func TestDescribeSheetFallsBackToHostWideEvenAfterAnExplicitEmptyDeviceSheet(t *testing.T) {
	ts, _, _, _ := newServer(t)

	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Sheet: ptr("# gpubox\nshared rack A1, ask #infra before touching cabling"),
		// This is what a real worker with no host.d/gpu0.md sends: gpu0 IS
		// present in the map, mapped to "" — not omitted, the way the test
		// above's bare RegisterRequest (no DeviceSheets field at all) does.
		DeviceSheets: map[string]string{"gpu0": ""},
	})
	reg.Body.Close()
	require.Equal(t, http.StatusOK, reg.StatusCode)

	resp := get(t, ts, "ctok", "/v1/devices/gpubox:gpu0/describe")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.DescribeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Contains(t, out.Sheet, "shared rack A1",
		"an explicit empty per-device sheet must still fall back to the host-wide one, not shadow it with nothing")
	require.True(t, out.SheetIsHostWide,
		"the fallback sheet must be flagged as host-wide even though gpu0 has its own (empty) host_docs row")
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
