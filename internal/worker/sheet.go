package worker

import (
	"log/slog"
	"os"
	"path/filepath"
)

// maxSheetBytes bounds a single usage sheet AFTER any truncation marker is
// appended, so a hand-edited host.md (or a per-device sheet) can never blow
// up what this worker pushes to the controller. The controller enforces the
// same cap server-side (413) as a second line of defence — a worker that
// truncates correctly never trips it, but a bug here or a future caller that
// bypasses readSheets must not be able to bloat the database.
const maxSheetBytes = 64 * 1024

// sheetTruncatedMarker is appended, on its own trailing line, to any sheet
// that had to be cut down to fit maxSheetBytes, so a reader of `rc describe`
// output can tell an abbreviated sheet from one that merely happens to end
// there.
const sheetTruncatedMarker = "\n...(truncated: sheet exceeds 64KB)\n"

// readSheets reads this host's usage-sheet documentation: one host-wide file
// at <dir>/host.md, applied to every device in `rc describe`, and one
// optional per-device file at <dir>/host.d/<name>.md for each device this
// worker declares. perDevice always has one entry per device in devices,
// even when that device's file does not exist (empty string), so a caller
// never has to special-case a missing entry.
//
// Neither file existing is not an error — most hosts, and most devices,
// will never have written one. A read failure for any OTHER reason
// (permission denied, a directory where a file was expected, ...) is logged
// and also treated as an empty sheet: this is documentation, never
// load-bearing for scheduling, so it must never be the thing that keeps a
// worker from registering.
func readSheets(dir string, devices []DeviceConfig) (host string, perDevice map[string]string, err error) {
	host = readSheetFile(filepath.Join(dir, "host.md"))

	perDevice = make(map[string]string, len(devices))
	for _, d := range devices {
		perDevice[d.Name] = readSheetFile(filepath.Join(dir, "host.d", d.Name+".md"))
	}
	return host, perDevice, nil
}

// readSheetFile reads and, if needed, truncates one sheet file.
func readSheetFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("read usage sheet", "path", path, "err", err)
		}
		return ""
	}
	return truncateSheet(path, string(b))
}

// truncateSheet caps body at maxSheetBytes, appending sheetTruncatedMarker
// when it had to cut anything, and logging loudly: an operator whose sheet
// silently lost its tail has no way to notice on their own.
func truncateSheet(path, body string) string {
	if len(body) <= maxSheetBytes {
		return body
	}
	slog.Warn("usage sheet exceeds size cap; truncated", "path", path, "size", len(body), "cap", maxSheetBytes)
	cut := maxSheetBytes - len(sheetTruncatedMarker)
	if cut < 0 {
		cut = 0
	}
	return body[:cut] + sheetTruncatedMarker
}
