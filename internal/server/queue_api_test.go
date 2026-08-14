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

func TestSubmitQueuesWhenDeviceIsBusy(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	defer first.Body.Close()
	require.Equal(t, http.StatusCreated, first.StatusCode)

	// Give the first job the device.
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusCreated, second.StatusCode, "a busy device queues, it does not 409")

	var job model.Job
	require.NoError(t, json.NewDecoder(second.Body).Decode(&job))
	require.Equal(t, model.JobQueued, job.State)
}

func TestSubmitWithNoWaitStillFailsFast(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	first.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b", NoWait: true,
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusConflict, second.StatusCode)
}

func TestJobViewReportsQueuePosition(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()

	view := get(t, ts, "ctok", "/v1/jobs/"+queued.ID)
	defer view.Body.Close()
	var out server.JobView
	require.NoError(t, json.NewDecoder(view.Body).Decode(&out))
	require.Equal(t, 1, out.QueuePosition)
	require.Equal(t, model.JobQueued, out.Job.State)
}

func TestSubmitRejectsRuntimeAboveDeviceCeiling(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)
	require.NoError(t, st.SetDeviceMaxRuntime("gpubox:gpu0", time.Hour))

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
		MaxRuntimeSeconds: 4 * 3600,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "runtime_above_ceiling", body["error"])
}

func TestKillCancelsAQueuedJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()

	kill := post(t, ts, "ctok", "/v1/jobs/"+queued.ID+"/kill", map[string]string{"submitter": "agent-b"})
	defer kill.Body.Close()
	require.Equal(t, http.StatusOK, kill.StatusCode)

	reloaded, err := st.Job(queued.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, reloaded.State)
}

func TestKillRefusesSomeoneElsesJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
	a.Body.Close()

	kill := post(t, ts, "ctok", "/v1/jobs/"+job.ID+"/kill", map[string]string{"submitter": "agent-b"})
	defer kill.Body.Close()
	require.Equal(t, http.StatusForbidden, kill.StatusCode)

	reloaded, err := st.Job(job.ID)
	require.NoError(t, err)
	require.NotEqual(t, model.JobKilled, reloaded.State)
	_ = st
}
