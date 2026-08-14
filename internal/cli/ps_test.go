package cli_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/mudler/agents-resources-controller/internal/cli"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// lineFor returns the single rendered table line naming id, failing the test
// if there isn't exactly one — so an assertion made against it is actually
// scoped to that device's row, not to the whole table.
func lineFor(t *testing.T, out, id string) string {
	t.Helper()
	var match string
	found := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, id) {
			found++
			match = line
		}
	}
	require.Equalf(t, 1, found, "expected exactly one line containing %q, got %d in:\n%s", id, found, out)
	return match
}

// failWriter fails every Write with a fixed error, simulating a broken pipe
// (`rc devices | head`) or a full disk.
type failWriter struct{ err error }

func (w failWriter) Write(p []byte) (int, error) { return 0, w.err }

// TestRenderDevicesPropagatesWriteError guards against the table being
// silently dropped: tabwriter buffers every row until Flush, so a discarded
// Flush error would let `rc devices` print nothing and still exit 0 on a
// broken pipe or full disk — exactly the kind of quiet lie this tool exists
// to avoid.
func TestRenderDevicesPropagatesWriteError(t *testing.T) {
	wantErr := errors.New("disk full")

	err := cli.RenderDevices(failWriter{wantErr}, []server.DeviceView{
		{Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady}},
	})

	require.ErrorIs(t, err, wantErr)
}

func TestRenderDevicesShowsHolderElapsedAndStaleness(t *testing.T) {
	var out bytes.Buffer

	err := cli.RenderDevices(&out, []server.DeviceView{
		{
			// Busy, holder attached, heartbeat FRESH (well under the 30s
			// grace): must render exactly like a healthy device — no "no
			// contact" anywhere on its line.
			Device:              model.Device{ID: "gpubox:gpu0", State: model.DeviceBusy},
			Holder:              "mudler@laptop/sess-1",
			Command:             []string{"./bench", "--fast"},
			ElapsedSeconds:      125,
			HeartbeatAgeSeconds: 3,
		},
		{
			// Busy, heartbeat STALE (past the 30s grace): this is the
			// signal a flock file cannot express — a worker gone quiet
			// while still holding a device.
			Device:              model.Device{ID: "gpubox:gpu1", State: model.DeviceBusy},
			Holder:              "other@host/sess-2",
			ElapsedSeconds:      10,
			HeartbeatAgeSeconds: 47,
		},
		{
			// Unhealthy, but heartbeat FRESH: the worker reconnected after
			// whatever fault marked the device unhealthy. It must still
			// show "unhealthy" (a fresh heartbeat does not clear that on
			// its own), but must NOT also claim it is out of contact — that
			// would contradict the heartbeat this very row is built from.
			Device:              model.Device{ID: "gpubox:gpu2", State: model.DeviceUnhealthy},
			HeartbeatAgeSeconds: 5,
		},
	})
	require.NoError(t, err)

	got := out.String()

	gpu0 := lineFor(t, got, "gpubox:gpu0")
	require.Contains(t, gpu0, "busy")
	require.Contains(t, gpu0, "mudler@laptop/sess-1")
	require.Contains(t, gpu0, "2m5s")
	require.Contains(t, gpu0, "./bench --fast")
	require.NotContains(t, gpu0, "no contact", "a device with a fresh heartbeat must not be flagged out of contact")

	gpu1 := lineFor(t, got, "gpubox:gpu1")
	require.Contains(t, gpu1, "no contact 47s")

	gpu2 := lineFor(t, got, "gpubox:gpu2")
	require.Contains(t, gpu2, "unhealthy")
	require.NotContains(t, gpu2, "no contact",
		"an unhealthy device with a fresh heartbeat must not also be reported as silent")
}

// TestRenderJobsListsQueuedJobsWithPositions covers the minor that `rc ps`
// showed only active jobs, which left a queued job's ID unobtainable — and
// `rc kill`, which the README points operators at, needs an ID. Position
// comes from the order the controller already returns the queue in, counted
// per device, so it matches what the job's own queue_position would say.
func TestRenderJobsListsQueuedJobsWithPositions(t *testing.T) {
	var out bytes.Buffer

	err := cli.RenderJobs(&out, &server.StateResponse{
		Jobs: []model.Job{{
			ID: "running-1", DeviceID: "gpubox:gpu0", State: model.JobRunning,
			Submitter: "agent-a", Command: []string{"./bench"},
		}},
		// Scheduling order, as /v1/state returns it: two jobs waiting on gpu0
		// and one on gpu1, whose own queue starts at 1 again.
		Queued: []model.Job{
			{ID: "queued-1", DeviceID: "gpubox:gpu0", State: model.JobQueued, Submitter: "agent-b", Command: []string{"./train"}},
			{ID: "queued-2", DeviceID: "gpubox:gpu0", State: model.JobQueued, Submitter: "agent-c", Command: []string{"./eval"}},
			{ID: "queued-3", DeviceID: "gpubox:gpu1", State: model.JobQueued, Submitter: "agent-d", Command: []string{"./other"}},
		},
	})
	require.NoError(t, err)

	got := out.String()
	require.Contains(t, lineFor(t, got, "running-1"), "running")

	first := lineFor(t, got, "queued-1")
	require.Contains(t, first, "queued (#1)")
	require.Contains(t, first, "agent-b")
	require.Contains(t, first, "./train")

	require.Contains(t, lineFor(t, got, "queued-2"), "queued (#2)")
	require.Contains(t, lineFor(t, got, "queued-3"), "queued (#1)",
		"position is per device: another device's queue starts at 1 again")
}

// The table is buffered until Flush, so a dropped Flush error would let
// `rc ps | head` print nothing and still exit 0.
func TestRenderJobsPropagatesWriteError(t *testing.T) {
	wantErr := errors.New("disk full")
	err := cli.RenderJobs(failWriter{wantErr}, &server.StateResponse{
		Jobs: []model.Job{{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobRunning}},
	})
	require.ErrorIs(t, err, wantErr)
}
