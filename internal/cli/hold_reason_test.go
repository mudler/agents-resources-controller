package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// holdCmdIsolated builds a hold command pointed at a test server that fails
// every request, so a validation bug shows up as a test failure rather than a
// real hold on the real fleet. An earlier version of this test omitted it,
// defaulted to http://localhost:8080, and queued an actual hold on dgx:gpu0.
func holdCmdIsolated(t *testing.T, args ...string) (*bytes.Buffer, func() error) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("hold contacted the controller (%s %s) instead of failing validation first", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)
	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := NewHoldCmd()
	cmd.SetArgs(args)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	return &out, cmd.Execute
}

// A hold with no reason is indistinguishable from a leak: rc devices shows a
// bare submitter name and nobody can tell whether the box is in use or simply
// forgotten. On this fleet 3 of the first 7 holds carried none.
func TestHoldRequiresAReason(t *testing.T) {
	_, run := holdCmdIsolated(t, "dgx:gpu0", "--ttl", "1h")
	err := run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--reason")
}

// Whitespace is not a reason. This is the shape of `--reason "$SOMETHING_UNSET"`,
// a mistake rather than a request for a blank one — the same rule
// defaultSubmitter already applies to RC_SUBMITTER.
func TestHoldRejectsAWhitespaceReason(t *testing.T) {
	_, run := holdCmdIsolated(t, "dgx:gpu0", "--ttl", "1h", "--reason", "   ")
	err := run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--reason")
}

// The reason must be checked before the ttl is, or a caller missing both is
// told to fix the ttl, fixes it, and is then told about the reason.
func TestHoldRejectsAMissingReasonEvenWithoutTTL(t *testing.T) {
	_, run := holdCmdIsolated(t, "dgx:gpu0")
	err := run()
	require.Error(t, err)
}
