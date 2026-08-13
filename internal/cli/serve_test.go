package cli_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/stretchr/testify/require"
)

// TestServeExitsPromptlyWhenListenFails guards against rc serve hanging
// silently on a bind failure. Before the fix, the reaper goroutine only ever
// learned to stop from cmd.Context() being cancelled by a signal; when
// ListenAndServe fails immediately (e.g. the port is already in use), RunE
// tries to return but blocks forever inside the deferred reaperWG.Wait(),
// since nothing else would ever cancel that context. Under systemd this
// means the unit never exits and Restart= never fires — a bind conflict
// looks like a hang, not a failure.
func TestServeExitsPromptlyWhenListenFails(t *testing.T) {
	// Occupy a real port first so rc serve's own bind fails the same way a
	// genuine port conflict would.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	t.Setenv("RC_TOKENS", "ctok:client")
	dataDir := t.TempDir()

	cmd := cli.NewServeCmd()
	cmd.SetArgs([]string{"--addr", addr, "--data", dataDir})
	cmd.SetContext(context.Background())
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "address already in use")
	case <-time.After(5 * time.Second):
		t.Fatal("rc serve did not return promptly when the listen address was already in use — it hung instead")
	}
}
