package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "worker.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// A worker in a container gets its token from a Secret, and the only sane way
// to hand a Secret to a process is the environment — writing it into the
// config file would mean templating a ConfigMap at deploy time or baking a
// credential into an image. RC_TOKEN is already what the client reads
// (cli.controllerToken), so the worker reading it too is one env var for the
// whole tool rather than a second convention.
func TestConfigTakesTheTokenFromTheEnvironmentWhenUnset(t *testing.T) {
	t.Setenv("RC_TOKEN", "from-the-secret")
	cfg, err := LoadConfig(writeCfg(t, "controller_url: http://c:8080\nhost: box\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.Equal(t, "from-the-secret", cfg.Token)
}

// An explicit token in the config file wins. Anything else would let a stray
// RC_TOKEN in a shell silently redirect a worker that was configured on disk.
func TestConfigPrefersAnExplicitTokenOverTheEnvironment(t *testing.T) {
	t.Setenv("RC_TOKEN", "from-the-secret")
	cfg, err := LoadConfig(writeCfg(t, "controller_url: http://c:8080\nhost: box\ntoken: from-the-file\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.Equal(t, "from-the-file", cfg.Token)
}

// Whitespace-only is the shape of `RC_TOKEN=$SOMETHING_UNSET`, a mistake
// rather than a request to register unauthenticated.
func TestConfigIgnoresABlankEnvironmentToken(t *testing.T) {
	t.Setenv("RC_TOKEN", "   ")
	cfg, err := LoadConfig(writeCfg(t, "controller_url: http://c:8080\nhost: box\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.Empty(t, cfg.Token)
}
