package worker_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
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
