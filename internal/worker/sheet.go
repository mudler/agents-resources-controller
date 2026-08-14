package worker

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"unicode/utf8"
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
// worker declares.
//
// Neither file existing is not an error — most hosts, and most devices,
// will never have written one — and perDevice gets a present, empty entry
// for a device whose file is simply missing, so a caller never has to
// special-case that. A read failure for any OTHER reason (permission
// denied, a directory where a file was expected, ...) is a DIFFERENT case:
// it is logged, and that device's key is omitted from perDevice entirely
// rather than reported as empty — "could not read this sheet" must never be
// indistinguishable from "there genuinely is no sheet", because a caller
// that can't tell them apart would overwrite a previously good, stored
// sheet with nothing the moment a chmod (or a transient EACCES) made this
// worker unable to read its own copy.
//
// The host sheet gets the same treatment via the returned err: err is
// non-nil only when host.md itself failed to read for a non-missing
// reason, in which case host is "" but must NOT be treated as "the host
// has no sheet" — the caller is expected to leave whatever is already
// stored for the host alone when err != nil, exactly as an omitted device
// key already does.
func readSheets(dir string, devices []DeviceConfig) (host string, perDevice map[string]string, err error) {
	hostBody, hostOK, hostErr := readSheetFile(filepath.Join(dir, "host.md"))
	if !hostOK {
		err = fmt.Errorf("read host sheet: %w", hostErr)
	}
	host = hostBody

	perDevice = make(map[string]string, len(devices))
	for _, d := range devices {
		body, ok, readErr := readSheetFile(filepath.Join(dir, "host.d", d.Name+".md"))
		if !ok {
			slog.Warn("read usage sheet; leaving the stored copy alone", "device", d.Name, "err", readErr)
			continue
		}
		perDevice[d.Name] = body
	}
	return host, perDevice, err
}

// readSheetFile reads and, if needed, truncates one sheet file. ok is true
// for both a genuinely absent file (body "") and a successful read;
// it is false only for a read error that is NOT "the file doesn't exist" —
// permission denied, a directory in the way, and the like — which the
// caller must treat as "unknown", not "empty".
func readSheetFile(path string) (body string, ok bool, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", true, nil
		}
		return "", false, err
	}
	return truncateSheet(path, string(b)), true, nil
}

// truncateSheet caps body at maxSheetBytes, appending sheetTruncatedMarker
// when it had to cut anything, and logging loudly: an operator whose sheet
// silently lost its tail has no way to notice on their own. The cut point is
// walked back to the nearest rune boundary first, so a multi-byte character
// (an em dash, an accented letter, an emoji in someone's changelog) straddling
// the cut is never split into invalid UTF-8 — encoding/json would otherwise
// re-encode the trailing partial rune as U+FFFD on the wire, growing the
// payload back past the cap the controller enforces and getting the whole
// registration rejected with 413, permanently locking that host out of the
// fleet over nothing worse than a stray non-ASCII character near the 64KB
// mark.
func truncateSheet(path, body string) string {
	if len(body) <= maxSheetBytes {
		return body
	}
	slog.Warn("usage sheet exceeds size cap; truncated", "path", path, "size", len(body), "cap", maxSheetBytes)
	cut := maxSheetBytes - len(sheetTruncatedMarker)
	if cut < 0 {
		cut = 0
	}
	for cut > 0 && !utf8.RuneStart(body[cut]) {
		cut--
	}
	return body[:cut] + sheetTruncatedMarker
}
