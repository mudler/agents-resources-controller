package cli_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/stretchr/testify/require"
)

func TestLogsCommandWritesStoredSnapshotToStdout(t *testing.T) {
	var queries []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RequestURI())
		switch r.URL.Path {
		case "/v1/jobs/job1":
			_, _ = w.Write([]byte(`{"id":"job1","stdio":"log"}`))
		case "/v1/jobs/job1/logs":
			_, _ = w.Write([]byte("stored output\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewLogsCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"job1"})
	require.NoError(t, cmd.Execute())
	require.Equal(t, "stored output\n", stdout.String())
	require.Empty(t, stderr.String())
	require.Equal(t, []string{"/v1/jobs/job1", "/v1/jobs/job1/logs?follow=false"}, queries)
}

func TestLogsCommandFollowUsesFollowRequest(t *testing.T) {
	var logsQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/jobs/job1" {
			_, _ = w.Write([]byte(`{"id":"job1","stdio":"log"}`))
			return
		}
		logsQuery = r.URL.RawQuery
		_, _ = w.Write([]byte("replayed then live\n"))
	}))
	defer ts.Close()
	t.Setenv("RC_CONTROLLER", ts.URL)

	cmd := cli.NewLogsCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--follow", "job1"})
	require.NoError(t, cmd.Execute())
	require.Equal(t, "follow=true", logsQuery)
	require.Equal(t, "replayed then live\n", out.String())
}

func TestLogsCommandInterruptedFollowReturns130WithoutDiagnostic(t *testing.T) {
	requestStarted := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/jobs/job1" {
			_, _ = w.Write([]byte(`{"id":"job1","stdio":"log"}`))
			return
		}
		close(requestStarted)
		<-r.Context().Done()
	}))
	defer ts.Close()
	t.Setenv("RC_CONTROLLER", ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cmd := cli.NewLogsCmd()
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"-f", "job1"})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	<-requestStarted
	cancel()
	err := <-done
	var coded interface{ ExitCode() int }
	require.ErrorAs(t, err, &coded)
	require.Equal(t, 130, coded.ExitCode())
	require.NotContains(t, err.Error(), "context canceled")
}
