package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeUserConfig points RC_CONFIG at a temp file and returns its path.
func writeUserConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	t.Setenv("RC_CONFIG", path)
	resetUserConfig()
	t.Cleanup(resetUserConfig)
	return path
}

func TestUserConfigSuppliesTokenWithoutEnv(t *testing.T) {
	t.Setenv("RC_TOKEN", "")
	t.Setenv("RC_CONTROLLER", "")
	writeUserConfig(t, "controller: http://box:9090\ntoken: filetok\n")

	// The whole point: a fresh shell that never sourced an env file still
	// authenticates. This is the bug the user hit.
	require.Equal(t, "filetok", controllerToken())
	require.Equal(t, "http://box:9090", controllerURL())
}

func TestEnvOverridesUserConfig(t *testing.T) {
	writeUserConfig(t, "controller: http://from-file\ntoken: filetok\n")
	t.Setenv("RC_TOKEN", "envtok")
	t.Setenv("RC_CONTROLLER", "http://from-env")

	require.Equal(t, "envtok", controllerToken())
	require.Equal(t, "http://from-env", controllerURL())
}

func TestMissingUserConfigFallsBackToDefaults(t *testing.T) {
	t.Setenv("RC_CONFIG", filepath.Join(t.TempDir(), "absent.yaml"))
	t.Setenv("RC_TOKEN", "")
	t.Setenv("RC_CONTROLLER", "")
	resetUserConfig()
	t.Cleanup(resetUserConfig)

	require.Equal(t, "", controllerToken())
	require.Equal(t, "http://localhost:8080", controllerURL())
}

// A malformed config must not be swallowed. Silently ignoring it reproduces
// the exact confusing failure this feature exists to remove: a token that is
// present on disk but somehow "unauthorized".
func TestMalformedUserConfigIsReported(t *testing.T) {
	writeUserConfig(t, "controller: [unclosed\n")
	t.Setenv("RC_TOKEN", "")

	_, err := loadUserConfig()
	require.Error(t, err)
	require.Contains(t, err.Error(), "config.yaml")
}

func TestUserConfigSuppliesSubmitter(t *testing.T) {
	t.Setenv("RC_SUBMITTER", "")
	writeUserConfig(t, "submitter: agent/from-file\n")

	require.Equal(t, "agent/from-file", defaultSubmitter())
}

// Env still wins for the submitter, so a per-session identity can override
// the machine-wide default.
func TestSubmitterEnvOverridesUserConfig(t *testing.T) {
	writeUserConfig(t, "submitter: agent/from-file\n")
	t.Setenv("RC_SUBMITTER", "agent/from-env")

	require.Equal(t, "agent/from-env", defaultSubmitter())
}

// Values are trimmed: a trailing newline inside a quoted YAML scalar is the
// classic way a pasted token stops matching.
func TestUserConfigTrimsValues(t *testing.T) {
	t.Setenv("RC_TOKEN", "")
	writeUserConfig(t, "token: \"  filetok  \"\n")

	require.Equal(t, "filetok", controllerToken())
}
