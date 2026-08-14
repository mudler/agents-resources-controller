package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A host.md is read as-is and applied host-wide.
func TestReadSheetsReadsHostSheet(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "host.md"), []byte("# gpubox\nshared rack A1"), 0o644))

	host, perDevice, err := readSheets(dir, []DeviceConfig{{Name: "gpu0"}})
	require.NoError(t, err)
	require.Equal(t, "# gpubox\nshared rack A1", host)
	require.Equal(t, "", perDevice["gpu0"])
}

// A per-device sheet at host.d/<name>.md is read for that device only.
func TestReadSheetsReadsPerDeviceSheet(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "host.d"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "host.d", "gpu0.md"), []byte("gpu0 runs the eval suite"), 0o644))

	host, perDevice, err := readSheets(dir, []DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}})
	require.NoError(t, err)
	require.Equal(t, "", host)
	require.Equal(t, "gpu0 runs the eval suite", perDevice["gpu0"])
	require.Equal(t, "", perDevice["gpu1"], "a device with no sheet file gets an empty entry, not a missing one")
}

// Neither file existing is the common case, not an error.
func TestReadSheetsMissingFilesAreNotAnError(t *testing.T) {
	dir := t.TempDir() // empty: no host.md, no host.d/

	host, perDevice, err := readSheets(dir, []DeviceConfig{{Name: "gpu0"}})
	require.NoError(t, err)
	require.Equal(t, "", host)
	require.Equal(t, "", perDevice["gpu0"])
}

// A sheet larger than the 64KB cap is truncated, not rejected: the marker is
// present and the total result never exceeds the cap.
func TestReadSheetsTruncatesOversizedSheet(t *testing.T) {
	dir := t.TempDir()
	huge := strings.Repeat("x", maxSheetBytes+5000)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "host.md"), []byte(huge), 0o644))

	host, _, err := readSheets(dir, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, len(host), maxSheetBytes, "truncated result must never exceed the cap")
	require.Contains(t, host, "truncated", "the truncation marker must be present")
	require.True(t, strings.HasSuffix(host, sheetTruncatedMarker),
		"the marker must trail whatever content survived truncation")
}
