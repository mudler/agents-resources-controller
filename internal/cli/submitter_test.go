package cli

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// RC_SUBMITTER exists for agents. An agent session wants to say who it is
// once, in its environment, rather than remember --as on every rc run, rc
// hold, rc kill and rc release — and forgetting it on the kill is what makes
// a kill fail with not_job_owner after the run succeeded. The flag still
// wins, so a one-off `--as` overrides the session's identity without
// unsetting anything.
func TestDefaultSubmitterPrefersRCSubmitter(t *testing.T) {
	t.Setenv("RC_SUBMITTER", "claude/sess-7f2a")
	t.Setenv("USER", "mudler")
	t.Setenv("CLAUDE_SESSION_ID", "should-be-ignored")

	require.Equal(t, "claude/sess-7f2a", defaultSubmitter(),
		"RC_SUBMITTER must win over the derived user@host/session identity")
}

func TestDefaultSubmitterIgnoresBlankRCSubmitter(t *testing.T) {
	t.Setenv("RC_SUBMITTER", "   ")
	t.Setenv("USER", "mudler")
	t.Setenv("CLAUDE_SESSION_ID", "")

	// A variable exported as empty (or whitespace) is the shape you get from
	// `export RC_SUBMITTER=$SOMETHING_UNSET`, which is a mistake rather than
	// a request for a blank identity — and a blank submitter would make the
	// ownership check on rc kill vacuous for every job on the fleet.
	require.NotEmpty(t, defaultSubmitter())
	require.NotEqual(t, "   ", defaultSubmitter())
}

func TestDefaultSubmitterTrimsRCSubmitter(t *testing.T) {
	t.Setenv("RC_SUBMITTER", "  agent-a  ")
	require.Equal(t, "agent-a", defaultSubmitter())
}

// Unset RC_SUBMITTER keeps the previous behaviour exactly.
//
// RC_SUBMITTER is not the only source that outranks the derived identity: so
// does ~/.config/rc/config.yaml, and without pointing RC_CONFIG somewhere
// empty this test reads whatever the person running it happens to have in
// their real home directory. It passed on a machine with no config and failed
// on a machine with one, which is the worst way for a test to be wrong.
func TestDefaultSubmitterFallsBackToUserHostSession(t *testing.T) {
	t.Setenv("RC_SUBMITTER", "")
	t.Setenv("RC_CONFIG", filepath.Join(t.TempDir(), "absent.yaml"))
	resetUserConfig()
	t.Cleanup(resetUserConfig)
	t.Setenv("USER", "mudler")
	t.Setenv("CLAUDE_SESSION_ID", "abc123")

	got := defaultSubmitter()
	require.Contains(t, got, "mudler@")
	require.Contains(t, got, "/abc123")
}
