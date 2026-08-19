package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func intPtr(i int) *int { return &i }

func TestSubmitReturnsErrNoDeviceOn409(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "no_device_available", "message": "gpubox:gpu0 is not free",
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	_, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"},
	})
	require.ErrorIs(t, err, client.ErrNoDevice)
}

func TestSubmitSendsBearerToken(t *testing.T) {
	// The handler runs on a goroutine spawned by net/http's server loop. A
	// bare `var gotAuth string` written there and read from the test
	// goroutine after Submit returns is a data race as far as -race is
	// concerned: nothing in that pair synchronizes through a primitive the
	// race detector tracks (network I/O completing isn't one). A buffered
	// channel send/receive is, so use that instead.
	authCh := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCh <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"id": "job1", "state": "assigned"})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	job, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"},
	})
	require.NoError(t, err)
	require.Equal(t, "job1", job.ID)

	select {
	case gotAuth := <-authCh:
		require.Equal(t, "Bearer ctok", gotAuth)
	case <-time.After(time.Second):
		t.Fatal("handler was never invoked")
	}
}

func TestJobsEncodesOnlySuppliedFilters(t *testing.T) {
	requestCh := make(chan *http.Request, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCh <- r.Clone(r.Context())
		_ = json.NewEncoder(w).Encode(server.JobsResponse{Jobs: []model.Job{{ID: "job1"}}})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "tok")
	jobs, err := c.Jobs(context.Background(), client.JobsOptions{
		Limit: 7, DeviceID: "gpu0", State: model.JobFailed,
	})
	require.NoError(t, err)
	require.Equal(t, []model.Job{{ID: "job1"}}, jobs)

	req := <-requestCh
	require.Equal(t, "/v1/jobs", req.URL.Path)
	require.Equal(t, "device=gpu0&limit=7&state=failed", req.URL.RawQuery)
	require.False(t, req.URL.Query().Has("submitter"))
}

func TestJobsOmitsAllZeroValueFilters(t *testing.T) {
	queryCh := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryCh <- r.URL.RawQuery
		_ = json.NewEncoder(w).Encode(server.JobsResponse{})
	}))
	defer ts.Close()

	_, err := client.New(ts.URL, "tok").Jobs(context.Background(), client.JobsOptions{})
	require.NoError(t, err)
	require.Empty(t, <-queryCh)
}

func TestWaitTerminalGivesUpWithClearError(t *testing.T) {
	// The job never leaves "assigned" — e.g. no worker ever attached to
	// the device — so WaitTerminal must give up rather than poll forever.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(server.JobView{Job: model.Job{ID: "job1", State: model.JobAssigned}})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "tok")
	start := time.Now()
	_, err := c.WaitTerminal(context.Background(), "job1", 300*time.Millisecond)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "job1")
	require.Contains(t, err.Error(), "assigned")
	require.Less(t, elapsed, 3*time.Second, "WaitTerminal must give up promptly, not hang")
}

func TestWaitTerminalToleratesTransientFailure(t *testing.T) {
	var calls int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(server.JobView{Job: model.Job{ID: "job1", State: model.JobSucceeded, ExitCode: intPtr(0)}})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "tok")
	job, err := c.WaitTerminal(context.Background(), "job1", 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, job.State)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2))
}
