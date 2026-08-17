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

// require_manual_clear is the one setting that governs autonomous recovery,
// and it reads from the config file the same way everything else does.
func TestConfigReadsRequireManualClearFromTheFile(t *testing.T) {
	cfg, err := LoadConfig(writeCfg(t,
		"controller_url: http://c:8080\nhost: box\nrequire_manual_clear: true\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.True(t, cfg.RequireManualClear)
}

// Same reasoning as RC_TOKEN: a containerised worker is configured by a
// ConfigMap it does not template, so a per-host decision has to be
// expressible in the environment.
func TestConfigTakesRequireManualClearFromTheEnvironment(t *testing.T) {
	t.Setenv("RC_REQUIRE_MANUAL_CLEAR", "true")
	cfg, err := LoadConfig(writeCfg(t, "controller_url: http://c:8080\nhost: box\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.True(t, cfg.RequireManualClear)
}

// Where this deliberately differs from RC_TOKEN: the environment cannot turn
// the setting OFF. A token has one correct value and the file is the
// authority on it; this is a safety catch, and a safety catch an env var can
// switch off is one that gets switched off by accident.
func TestEnvironmentCannotSwitchOffRequireManualClear(t *testing.T) {
	t.Setenv("RC_REQUIRE_MANUAL_CLEAR", "false")
	cfg, err := LoadConfig(writeCfg(t,
		"controller_url: http://c:8080\nhost: box\nrequire_manual_clear: true\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.True(t, cfg.RequireManualClear, "no source may make this worker less cautious than the file asked for")
}

// A value that is set but not a boolean is read as TRUE, which is the
// opposite of what a parser normally does and is the entire point: somebody
// typed something into a switch whose only job is to be more careful.
func TestUnparseableEnvironmentValueIsTreatedAsRequiringManualClear(t *testing.T) {
	t.Setenv("RC_REQUIRE_MANUAL_CLEAR", "yes please")
	cfg, err := LoadConfig(writeCfg(t, "controller_url: http://c:8080\nhost: box\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.True(t, cfg.RequireManualClear)
}

// Unset or blank is the shape of `RC_REQUIRE_MANUAL_CLEAR=$SOMETHING_UNSET`,
// a mistake rather than a request, and must not silently take a fleet's
// auto-recovery away.
func TestBlankEnvironmentValueDoesNotRequireManualClear(t *testing.T) {
	t.Setenv("RC_REQUIRE_MANUAL_CLEAR", "   ")
	cfg, err := LoadConfig(writeCfg(t, "controller_url: http://c:8080\nhost: box\ndevices: [{name: gpu0}]\n"))
	require.NoError(t, err)
	require.False(t, cfg.RequireManualClear)
}
