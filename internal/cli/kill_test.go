package cli_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/stretchr/testify/require"
)

// TestAttachExitsQuietlyOnCtrlC checks the Minor review finding: Ctrl-C is
// the normal, expected way to stop watching a job's output with `rc
// attach` — unlike `rc run`, there is no lease to protect and nothing to
// warn about, so it must exit with the conventional SIGINT status (130)
// rather than surfacing the raw "context canceled" plumbing error (which
// previously landed on main's generic exit-1 fallback).
func TestAttachExitsQuietlyOnCtrlC(t *testing.T) {
	reached := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case reached <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewAttachCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"job1"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-reached
		cancel()
	}()

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.True(t, errors.As(err, &coded), "expected a coded exit error for Ctrl-C, got: %v", err)
	require.Equal(t, 130, coded.ExitCode())
}

// TestAttachPropagatesRealErrors makes sure the Ctrl-C quieting above
// doesn't also swallow genuine failures (e.g. the controller rejecting an
// unknown job id) when there was no interrupt in play.
func TestAttachPropagatesRealErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewAttachCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"job1"})

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.False(t, errors.As(err, &coded), "a real failure must not be reported as a coded Ctrl-C exit")
}

// TestKillHelpRenders is a minimal smoke test that the command wires up
// without panicking; the end-to-end kill request path is covered at the
// client level (internal/client/queue_client_test.go) and via rc run's own
// use of Kill (internal/cli/run_queue_test.go).
func TestKillHelpRenders(t *testing.T) {
	cmd := cli.NewKillCmd()
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())
}
