package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

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
	gpu1Body, gpu1Present := perDevice["gpu1"]
	require.True(t, gpu1Present, "a device with no sheet file gets a PRESENT, empty entry, not a missing one")
	require.Equal(t, "", gpu1Body)
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

// TestReadSheetsTruncatesOversizedSheetAtARuneBoundary is the fix-round-1
// finding, pinned directly: a byte-offset cut can land in the middle of a
// multi-byte UTF-8 rune. A 3-byte character (an em dash, "€", any non-ASCII
// text an operator might reasonably paste into documentation) sitting
// exactly across the cut point used to produce invalid UTF-8; encoding/json
// then re-encodes the trailing partial rune as U+FFFD (3 bytes) on the wire,
// which can grow the payload back OVER the cap the controller enforces —
// getting the whole registration rejected with 413 and permanently locking
// that host out of the fleet. The fix must never emit invalid UTF-8, no
// matter where a multi-byte rune happens to fall relative to the cut.
func TestReadSheetsTruncatesOversizedSheetAtARuneBoundary(t *testing.T) {
	dir := t.TempDir()
	// "€" (U+20AC) is 3 bytes in UTF-8. Repeating it fills well past the cap
	// with every single rune 3 bytes wide, so SOME copy is guaranteed to
	// straddle whatever byte offset the cut lands on.
	huge := strings.Repeat("€", (maxSheetBytes/3)+2000)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "host.md"), []byte(huge), 0o644))

	host, _, err := readSheets(dir, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, len(host), maxSheetBytes, "truncated result must never exceed the cap")
	require.True(t, utf8.ValidString(host), "truncation must never split a multi-byte rune into invalid UTF-8")
	require.True(t, strings.HasSuffix(host, sheetTruncatedMarker))
}

// TestReadSheetPermissionErrorLeavesDeviceEntryAbsent is the other
// fix-round-1 finding: a read failure that is NOT "the file doesn't exist"
// (permission denied here) must not be reported the same way a genuinely
// missing sheet is — the caller (register/pushLabels) needs to be able to
// tell "empty on purpose" from "couldn't tell", or a transient chmod
// silently erases a previously stored, perfectly good sheet the next time
// this worker pushes.
func TestReadSheetPermissionErrorLeavesDeviceEntryAbsent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions; this test needs an unprivileged reader")
	}
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "host.d"), 0o755))
	p := filepath.Join(dir, "host.d", "gpu0.md")
	require.NoError(t, os.WriteFile(p, []byte("previously good content"), 0o000))
	t.Cleanup(func() { os.Chmod(p, 0o644) }) // let TempDir cleanup remove it

	_, perDevice, err := readSheets(dir, []DeviceConfig{{Name: "gpu0"}})
	require.NoError(t, err, "a per-device read failure must not fail the whole call")
	_, present := perDevice["gpu0"]
	require.False(t, present,
		"a device sheet that could not be read must be OMITTED, not reported as empty — an empty report would clear the stored copy")
}

// TestReadHostSheetPermissionErrorIsReportedAsAnError mirrors the device
// case for the host-wide sheet: readSheets' err return is exactly the
// signal a caller uses to avoid sending an unconditional (and therefore
// erasing) update for host.md when it could not actually be read.
func TestReadHostSheetPermissionErrorIsReportedAsAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions; this test needs an unprivileged reader")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "host.md")
	require.NoError(t, os.WriteFile(p, []byte("previously good content"), 0o000))
	t.Cleanup(func() { os.Chmod(p, 0o644) })

	host, _, err := readSheets(dir, nil)
	require.Error(t, err, "an unreadable host sheet must be reported, not silently folded into an empty string")
	require.Equal(t, "", host)
}
