package server_test

import (
	"strings"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// A registration carrying detected and declared labels stores each under
// the right provenance, and a host-wide fact (the "" key) reaches every
// device declared in that registration.
func TestRegisterStoresDetectedAndDeclaredLabels(t *testing.T) {
	ts, st, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host:    "gpubox",
		Devices: []server.DeviceSpec{{Name: "gpu0"}, {Name: "gpu1"}},
		Labels: map[string]map[string]string{
			"":     {"rack": "a1"},
			"gpu0": {"vendor": "nvidia"},
		},
		DeclaredLabels: map[string]map[string]string{
			"gpu0": {"owner": "team-a"},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	gpu0, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	byKey := map[string]model.Label{}
	for _, l := range gpu0 {
		byKey[l.Key+"/"+l.Source] = l
	}
	require.Equal(t, "nvidia", byKey["vendor/detected"].Value)
	require.Equal(t, "a1", byKey["rack/detected"].Value, "the host-wide fact must reach every device")
	require.Equal(t, "team-a", byKey["owner/declared"].Value)

	// gpu1 got no device-scoped facts of its own, but must still inherit the
	// host-wide one.
	gpu1, err := st.LabelsFor("gpubox:gpu1")
	require.NoError(t, err)
	require.Len(t, gpu1, 1)
	require.Equal(t, "rack", gpu1[0].Key)
	require.Equal(t, "a1", gpu1[0].Value)
	require.Equal(t, model.SourceDetected, gpu1[0].Source)
}

// A device-scoped value wins over a host-wide one on a key collision: the
// device fact is more specific and must not be shadowed.
func TestRegisterDeviceLabelWinsOverHostWideCollision(t *testing.T) {
	ts, st, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host:    "gpubox",
		Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels: map[string]map[string]string{
			"":     {"driver": "550.54"},
			"gpu0": {"driver": "560.10"},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	labels, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 1)
	require.Equal(t, "560.10", labels[0].Value, "the device-scoped value must win the collision")
}

// A registration carrying a usage sheet stores both the host-wide sheet and
// a per-device one.
func TestRegisterStoresUsageSheets(t *testing.T) {
	ts, st, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host:         "gpubox",
		Devices:      []server.DeviceSpec{{Name: "gpu0"}},
		Sheet:        "# gpubox\nshared rack A1",
		DeviceSheets: map[string]string{"gpu0": "gpu0 runs the eval suite"},
	})
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	hostBody, _, err := st.HostDoc("gpubox", "")
	require.NoError(t, err)
	require.Equal(t, "# gpubox\nshared rack A1", hostBody)

	deviceBody, _, err := st.HostDoc("gpubox", "gpubox:gpu0")
	require.NoError(t, err)
	require.Equal(t, "gpu0 runs the eval suite", deviceBody)
}

// An oversized sheet (over 64KB) is rejected outright with 413, and nothing
// from that registration — sheet, labels, or the device/worker rows
// themselves — is stored: a hand-edited file must not be able to bloat the
// database, and a rejected request must not partially apply.
func TestRegisterRejectsOversizedSheet(t *testing.T) {
	ts, st, _, _ := newServer(t)

	huge := strings.Repeat("x", (64*1024)+1)
	resp := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host:    "gpubox",
		Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Sheet:   huge,
		Labels:  map[string]map[string]string{"gpu0": {"vendor": "nvidia"}},
	})
	defer resp.Body.Close()
	require.Equal(t, 413, resp.StatusCode)

	hostBody, at, err := st.HostDoc("gpubox", "")
	require.NoError(t, err)
	require.Empty(t, hostBody)
	require.True(t, at.IsZero())

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Empty(t, devices, "the worker/device rows must not be created by a rejected registration")

	labels, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Empty(t, labels)
}

// An oversized PER-DEVICE sheet is rejected the same way as an oversized
// host sheet.
func TestRegisterRejectsOversizedDeviceSheet(t *testing.T) {
	ts, _, _, _ := newServer(t)

	huge := strings.Repeat("x", (64*1024)+1)
	resp := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host:         "gpubox",
		Devices:      []server.DeviceSpec{{Name: "gpu0"}},
		DeviceSheets: map[string]string{"gpu0": huge},
	})
	defer resp.Body.Close()
	require.Equal(t, 413, resp.StatusCode)
}

// A registration that explicitly reports an empty (but non-nil) label set
// for a device — a probe that ran fine and legitimately found nothing this
// round, e.g. a card was removed — clears whatever was previously detected
// for that device. This is the store's own documented replace semantics
// (see store.ReplaceLabels) applied faithfully at the HTTP layer: the
// controller cannot second-guess an explicit report.
func TestRegistrationWithExplicitEmptyLabelsClearsPreviouslyDetected(t *testing.T) {
	ts, st, _, _ := newServer(t)

	first := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels: map[string]map[string]string{"gpu0": {"vendor": "nvidia"}},
	})
	first.Body.Close()
	require.Equal(t, 200, first.StatusCode)

	before, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, before, 1, "sanity: the first registration actually stored the label")

	// The card was removed: the second registration explicitly says "gpu0
	// has zero detected labels now" rather than omitting the field.
	second := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels: map[string]map[string]string{"gpu0": {}},
	})
	second.Body.Close()
	require.Equal(t, 200, second.StatusCode)

	after, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Empty(t, after, "an explicit empty report must clear the previously detected set")
}

// The other half of the same contract, and the one the CARRIED-FORWARD
// review finding is actually about: a registration that omits the labels
// field entirely — Go's zero value, absent from the wire — must NOT be
// read as "detected nothing". The controller cannot tell that apart from a
// probe pass that failed outright, so it must do the only safe thing:
// leave whatever is already stored untouched. If this test is broken by
// changing handleRegister to unconditionally call ReplaceLabels with
// req.Labels (a nil map, which store.ReplaceLabels treats identically to an
// empty one — see TestReplaceLabelsWithEmptyMapClearsThatSource in
// internal/store), it must fail.
func TestRegistrationWithoutLabelsFieldPreservesPreviouslyDetected(t *testing.T) {
	ts, st, _, _ := newServer(t)

	first := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels: map[string]map[string]string{"gpu0": {"vendor": "nvidia"}},
	})
	first.Body.Close()
	require.Equal(t, 200, first.StatusCode)

	// A bare re-registration with no Labels field set at all — exactly what
	// a worker whose probe pass produced nothing sends (see
	// worker.labelsPayload).
	second := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
	})
	second.Body.Close()
	require.Equal(t, 200, second.StatusCode)

	after, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, after, 1, "a registration silent about labels must never wipe what was already detected")
	require.Equal(t, "nvidia", after[0].Value)
}

// The dedicated periodic-push route stores labels and sheets the same way
// registration does, without requiring a full re-registration.
func TestPushLabelsStoresDetectedLabelsAndSheet(t *testing.T) {
	ts, st, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	resp := post(t, ts, "wtok", "/v1/workers/"+workerID+"/labels", server.LabelsPushRequest{
		Host:    "gpubox",
		Devices: []string{"gpu0"},
		Labels:  map[string]map[string]string{"gpu0": {"vendor": "nvidia"}},
		Sheet:   "# gpubox",
	})
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	labels, err := st.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 1)
	require.Equal(t, "nvidia", labels[0].Value)

	hostBody, _, err := st.HostDoc("gpubox", "")
	require.NoError(t, err)
	require.Equal(t, "# gpubox", hostBody)
}

// The whole reason handlePushLabels exists as a route separate from
// handleRegister: a live worker's periodic push must never be able to reap
// its own currently-running job. Calling /v1/workers/register again from a
// live process does exactly that (UpsertWorker's reapInFlightJobsLocked
// treats every re-registration as a fresh process with nothing running),
// so this pins that the labels-push route leaves an in-flight job and its
// device completely alone.
func TestPushLabelsDoesNotDisturbARunningJob(t *testing.T) {
	ts, st, _, c := newServer(t)
	workerID := registerWorker(t, ts)

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"sleep", "300"}, Submitter: "agent-a",
		LeaseTTL: 5 * time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, st.MarkRunning(job.ID, c.Now()))

	resp := post(t, ts, "wtok", "/v1/workers/"+workerID+"/labels", server.LabelsPushRequest{
		Host: "gpubox", Devices: []string{"gpu0"},
		Labels: map[string]map[string]string{"gpu0": {"vendor": "nvidia"}},
	})
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	reloaded, err := st.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobRunning, reloaded.State, "the periodic label push must never reap a job it did not touch")

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Len(t, devices, 1)
	require.Equal(t, model.DeviceBusy, devices[0].State)
}

// An oversized sheet on the push route is rejected exactly like on
// registration.
func TestPushLabelsRejectsOversizedSheet(t *testing.T) {
	ts, _, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	huge := strings.Repeat("x", (64*1024)+1)
	resp := post(t, ts, "wtok", "/v1/workers/"+workerID+"/labels", server.LabelsPushRequest{
		Host: "gpubox", Devices: []string{"gpu0"}, Sheet: huge,
	})
	defer resp.Body.Close()
	require.Equal(t, 413, resp.StatusCode)
}
