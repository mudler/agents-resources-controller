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
