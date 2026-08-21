package store_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func enqueueHistoryJob(t *testing.T, s *store.Store, deviceID, submitter string, priority int) model.Job {
	t.Helper()
	job, err := s.Enqueue(store.EnqueueRequest{
		DeviceID:       deviceID,
		Command:        []string{"./bench", "--priority"},
		Cwd:            "/workspace",
		Env:            map[string]string{"RUN": "history"},
		Submitter:      submitter,
		IdempotencyKey: submitter + deviceID + strconv.Itoa(priority),
		Priority:       priority,
		MaxRuntime:     17 * time.Minute,
		IdleTimeout:    3 * time.Minute,
		Kind:           model.LeaseKindJob,
		Reason:         "history test",
		Stdio:          model.StdioLogs,
	})
	require.NoError(t, err)
	return *job
}

func TestListJobsFiltersAndOrdersNewestFirst(t *testing.T) {
	s, c := newStore(t)
	registerSecondDevice(t, s)

	completed, err := s.Allocate(req("carol"))
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(completed.ID, c.Now()))
	c.Advance(time.Minute)
	exitCode := 0
	require.NoError(t, s.Release(completed.ID, model.JobSucceeded, &exitCode, "finished"))
	c.Advance(time.Minute)
	old := enqueueHistoryJob(t, s, "gpubox:gpu0", "alice", 1)
	c.Advance(time.Minute)
	bob := enqueueHistoryJob(t, s, "gpubox:gpu1", "bob", 2)
	c.Advance(time.Minute)
	alice := enqueueHistoryJob(t, s, "gpubox:gpu1", "alice", 3)
	// Deliberately share submitted_at so the secondary ID ordering is tested.
	tied := enqueueHistoryJob(t, s, "gpubox:gpu1", "alice", 4)

	tests := []struct {
		name   string
		filter store.JobFilter
		want   []string
	}{
		{name: "device", filter: store.JobFilter{Limit: 10, DeviceID: "gpubox:gpu0"}, want: []string{old.ID, completed.ID}},
		{name: "submitter", filter: store.JobFilter{Limit: 10, Submitter: "bob"}, want: []string{bob.ID}},
		{name: "state", filter: store.JobFilter{Limit: 10, State: model.JobQueued}, want: nil},
		{name: "combined", filter: store.JobFilter{Limit: 10, DeviceID: "gpubox:gpu1", Submitter: "bob", State: model.JobQueued}, want: []string{bob.ID}},
		{name: "empty", filter: store.JobFilter{Limit: 10, Submitter: "nobody"}, want: []string{}},
	}
	if tied.ID > alice.ID {
		tests[2].want = []string{tied.ID, alice.ID, bob.ID, old.ID}
	} else {
		tests[2].want = []string{alice.ID, tied.ID, bob.ID, old.ID}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := s.ListJobs(tt.filter)
			require.NoError(t, err)
			ids := make([]string, 0, len(jobs))
			for _, job := range jobs {
				ids = append(ids, job.ID)
			}
			require.Equal(t, tt.want, ids)
		})
	}

	completedFromJob, err := s.Job(completed.ID)
	require.NoError(t, err)
	completedFromList, err := s.ListJobs(store.JobFilter{Limit: 10, State: model.JobSucceeded})
	require.NoError(t, err)
	require.Equal(t, []model.Job{*completedFromJob}, completedFromList,
		"terminal fields, including timestamps and exit status, must decode identically")
	require.NotNil(t, completedFromList[0].StartedAt)
	require.NotNil(t, completedFromList[0].FinishedAt)
	require.NotNil(t, completedFromList[0].ExitCode)
}

func TestListJobsLimitBoundsAndEnforcement(t *testing.T) {
	s, c := newStore(t)
	first := enqueueHistoryJob(t, s, "gpubox:gpu0", "alice", 1)
	c.Advance(time.Minute)
	second := enqueueHistoryJob(t, s, "gpubox:gpu0", "alice", 2)

	jobs, err := s.ListJobs(store.JobFilter{Limit: 1})
	require.NoError(t, err, "one is the valid lower limit boundary")
	require.Len(t, jobs, 1)
	require.Equal(t, second.ID, jobs[0].ID)
	require.NotEqual(t, first.ID, jobs[0].ID)

	jobs, err = s.ListJobs(store.JobFilter{Limit: 200})
	require.NoError(t, err, "200 is the valid upper limit boundary")
	require.Len(t, jobs, 2)

	for _, limit := range []int{-1, 0, 201} {
		t.Run(time.Duration(limit).String(), func(t *testing.T) {
			jobs, err := s.ListJobs(store.JobFilter{Limit: limit})
			require.Error(t, err)
			require.Nil(t, jobs)
		})
	}
}

func TestListJobsDecodesEveryJobFieldLikeJob(t *testing.T) {
	s, _ := newStore(t)
	want := enqueueHistoryJob(t, s, "gpubox:gpu0", "alice", 7)

	fromJob, err := s.Job(want.ID)
	require.NoError(t, err)
	jobs, err := s.ListJobs(store.JobFilter{Limit: 1})
	require.NoError(t, err)
	require.Equal(t, *fromJob, jobs[0])
	require.Equal(t, want, jobs[0])

	recent, err := s.RecentJobsForDevice("gpubox:gpu0", 1)
	require.NoError(t, err)
	require.Equal(t, jobs, recent)
}
