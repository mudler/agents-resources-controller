package worker_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "worker.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// Stage 1 configs are plain strings and must keep working.
func TestLoadConfigAcceptsPlainDeviceNames(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - gpu0
  - gpu1
`))
	require.NoError(t, err)
	require.Len(t, cfg.Devices, 2)
	require.Equal(t, "gpu0", cfg.Devices[0].Name)
	require.Zero(t, cfg.Devices[0].MaxRuntime, "no ceiling declared")
}

func TestLoadConfigAcceptsDeviceObjectsWithCeilings(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - name: gpu0
    max_runtime: 4h
  - name: gpu1
`))
	require.NoError(t, err)
	require.Len(t, cfg.Devices, 2)
	require.Equal(t, "gpu0", cfg.Devices[0].Name)
	require.Equal(t, 4*time.Hour, cfg.Devices[0].MaxRuntime)
	require.Equal(t, "gpu1", cfg.Devices[1].Name)
	require.Zero(t, cfg.Devices[1].MaxRuntime)
}

func TestLoadConfigRejectsADeviceWithNoName(t *testing.T) {
	_, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - max_runtime: 4h
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "name")
}

// TestLoadConfigRejectsSubSecondMaxRuntime guards the fix for the review
// finding that int(d.MaxRuntime.Seconds()) truncates a sub-second ceiling
// (e.g. 500ms) to 0, which the server then silently treats as "no ceiling"
// declared at all rather than the sub-second one the operator wrote. The
// column is seconds by design, so this must be rejected at config load, not
// silently rounded away, and the error must name the offending device.
func TestLoadConfigRejectsSubSecondMaxRuntime(t *testing.T) {
	_, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - name: gpu0
    max_runtime: 500ms
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "gpu0")
	require.Contains(t, err.Error(), "second")
}

// Host-level hooks defaults apply to every device that doesn't override them,
// and the documented defaults (60s timeout, 30s linger) apply when the
// operator gives no "hooks:" section at all.
func TestLoadConfigAppliesHostHookDefaults(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
hooks:
  timeout: 90s
  release_linger: 45s
devices:
  - name: gpu0
    on_acquire: /etc/rc/hooks/stop.sh
    on_release: /etc/rc/hooks/start.sh
`))
	require.NoError(t, err)
	require.Len(t, cfg.Devices, 1)
	require.Equal(t, "/etc/rc/hooks/stop.sh", cfg.Devices[0].OnAcquire)
	require.Equal(t, "/etc/rc/hooks/start.sh", cfg.Devices[0].OnRelease)
	require.Equal(t, 90*time.Second, cfg.Devices[0].HookTimeout)
	require.Equal(t, 45*time.Second, cfg.Devices[0].ReleaseLinger)
}

func TestLoadConfigDefaultsHookTimeoutAndLingerWhenUnset(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - name: gpu0
    on_acquire: /etc/rc/hooks/stop.sh
`))
	require.NoError(t, err)
	require.Equal(t, 60*time.Second, cfg.Devices[0].HookTimeout)
	require.Equal(t, 30*time.Second, cfg.Devices[0].ReleaseLinger)
}

// A per-device timeout/linger overrides the host default for that device
// only; a sibling device with none declared still gets the host value.
func TestLoadConfigPerDeviceHookOverrideWinsOverHostDefault(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
hooks:
  timeout: 90s
  release_linger: 45s
devices:
  - name: gpu0
    on_acquire: /etc/rc/hooks/stop.sh
    timeout: 5s
    release_linger: 2s
  - name: gpu1
    on_acquire: /etc/rc/hooks/stop.sh
`))
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, cfg.Devices[0].HookTimeout)
	require.Equal(t, 2*time.Second, cfg.Devices[0].ReleaseLinger)
	require.Equal(t, 90*time.Second, cfg.Devices[1].HookTimeout)
	require.Equal(t, 45*time.Second, cfg.Devices[1].ReleaseLinger)
}

// Only on_acquire, or only on_release, must each be independently valid —
// neither hook requires the other.
func TestLoadConfigAllowsEitherHookAlone(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - name: gpu0
    on_acquire: /etc/rc/hooks/stop.sh
  - name: gpu1
    on_release: /etc/rc/hooks/start.sh
`))
	require.NoError(t, err)
	require.Equal(t, "/etc/rc/hooks/stop.sh", cfg.Devices[0].OnAcquire)
	require.Empty(t, cfg.Devices[0].OnRelease)
	require.Empty(t, cfg.Devices[1].OnAcquire)
	require.Equal(t, "/etc/rc/hooks/start.sh", cfg.Devices[1].OnRelease)
}

// Plain-string device entries are shorthand for "no hooks" and must keep
// working unchanged.
func TestLoadConfigPlainStringDeviceHasNoHooks(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - gpu0
`))
	require.NoError(t, err)
	require.Empty(t, cfg.Devices[0].OnAcquire)
	require.Empty(t, cfg.Devices[0].OnRelease)
	// Even a hook-less device gets defaults resolved, harmlessly: nothing
	// ever reads them since no hook is configured to use them.
	require.Equal(t, 60*time.Second, cfg.Devices[0].HookTimeout)
	require.Equal(t, 30*time.Second, cfg.Devices[0].ReleaseLinger)
}

// A hook path that is present but blank (or whitespace-only) is rejected at
// load time rather than silently treated as "no hook" — an operator who
// typos an empty value should see it immediately, not discover it the first
// time a job tries to run on that device.
func TestLoadConfigRejectsBlankHookPath(t *testing.T) {
	_, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - name: gpu0
    on_acquire: "   "
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "gpu0")
	require.Contains(t, err.Error(), "on_acquire")
}

// The hook path itself is never stat'd at load time: the script may
// legitimately not exist yet (deployed later, e.g. by config management
// running after the worker starts).
func TestLoadConfigDoesNotRequireHookFileToExist(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - name: gpu0
    on_acquire: /this/path/does/not/exist.sh
`))
	require.NoError(t, err)
	require.Equal(t, "/this/path/does/not/exist.sh", cfg.Devices[0].OnAcquire)
}
