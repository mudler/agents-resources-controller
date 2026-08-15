package server_test

import (
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDashboardIsServedWithoutAToken(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp, err := ts.Client().Get(ts.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/html")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "resource controller")
	// The page must not embed a token.
	require.NotContains(t, strings.ToLower(string(body)), "bearer ct")
}

// dashboardBody fetches the page exactly as a browser would, so every
// assertion below is made against the bytes that actually ship rather than
// against the file on disk.
func dashboardBody(t *testing.T) string {
	t.Helper()
	ts, _, _, _ := newServer(t)

	resp, err := ts.Client().Get(ts.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

// TestDashboardNeverTouchesLocalStorage pins the storage half of the page's
// credential story: sessionStorage dies with the tab, localStorage does not.
// A client token written to localStorage would outlive the browsing session
// on a shared operator workstation, so the page must not name that API at
// all — not for the token, not for a UI preference, not anywhere.
func TestDashboardNeverTouchesLocalStorage(t *testing.T) {
	require.NotContains(t, dashboardBody(t), "localStorage")
}

var sessionSetItem = regexp.MustCompile(`sessionStorage\.setItem\(`)
var sessionSetItemLiteralKey = regexp.MustCompile(`sessionStorage\.setItem\("([^"]*)"`)

// TestDashboardStoresOnlyTheClientTokenInSessionStorage is the other half:
// the ONE thing the page is allowed to keep for the tab is the client token,
// which reaches only the operator's own jobs. The admin token used to clear a
// device must never be written anywhere, so any second key here — or a
// computed key whose value cannot be read off the source — is a failure.
func TestDashboardStoresOnlyTheClientTokenInSessionStorage(t *testing.T) {
	body := dashboardBody(t)

	writes := sessionSetItem.FindAllString(body, -1)
	require.NotEmpty(t, writes, "the page should still store the client token for the tab")

	keyed := sessionSetItemLiteralKey.FindAllStringSubmatch(body, -1)
	require.Len(t, keyed, len(writes),
		"every sessionStorage.setItem must use a literal key, so this test can see what is stored")
	for _, m := range keyed {
		require.Equal(t, "rc_token", m[1], "unexpected sessionStorage key written by the dashboard")
	}
}

// TestDashboardPromptsForTheAdminTokenAndKeepsNothing pins the admin path:
// the token is asked for at the moment of the click, handed to exactly one
// request, and dropped before any promise callback could capture it. The
// nulling line is asserted directly because it is load-bearing, not
// decorative — without it the value stays reachable from the closure for as
// long as the page lives.
func TestDashboardPromptsForTheAdminTokenAndKeepsNothing(t *testing.T) {
	body := dashboardBody(t)

	require.Contains(t, body, "window.prompt(", "clearing a device must prompt for an admin token")
	require.Contains(t, body, `"/v1/devices/" + encodeURIComponent(id) + "/clear"`)
	require.Contains(t, body, `Authorization: "Bearer " + admin`,
		"the clear request must use the just-prompted value")
	// Anchored to the start of a line rather than asserted as a plain
	// substring: `require.Contains(body, "admin = null;")` also passes when
	// the drop has been commented out, which is exactly the regression this
	// is here to catch. Verified by commenting the line out and watching
	// this trip.
	require.Regexp(t, `(?m)^\s*admin = null;`, body,
		"the prompted admin token must be dropped before any callback can capture it")
	require.Equal(t, 1, strings.Count(body, "window.prompt("),
		"only the admin-token prompt should exist")
}

// assignment matches `target = value` (not ==, ===, !=, <=, >= or =>) and
// captures the assignment target and everything up to the statement's
// semicolon.
var assignment = regexp.MustCompile(`(?s)([A-Za-z_$][\w$.]*)\s*=[^=>]([^;]*)`)

// quoted matches a string literal, stripped from an assignment's right-hand
// side before looking for the admin token there: `var note = "admin token
// used";` copies nothing, and a test that cannot tell it from `stash =
// admin;` would be noise rather than a guard.
var quoted = regexp.MustCompile(`"[^"]*"|'[^']*'`)

var adminIdentifier = regexp.MustCompile(`\badmin\b`)

// TestDashboardNeverCopiesTheAdminTokenAnywhere closes the one regression
// the assertions above cannot see. Keeping `admin = null;` exactly where it
// is, while adding a module-level `var lastAdmin` and `lastAdmin = admin;`
// just before the drop, passes every other test on this page and retains
// the admin token for the life of the tab — and it is exactly what someone
// adding a "retry the clear" affordance would write.
//
// So: between the prompt and the drop, `admin` may be assigned TO (it is
// trimmed there), but its value may never be assigned to anything else.
// Only that window is examined, because that is the only place in the file
// where the identifier holds a real token at all.
func TestDashboardNeverCopiesTheAdminTokenAnywhere(t *testing.T) {
	body := dashboardBody(t)

	start := strings.Index(body, "window.prompt(")
	require.NotEqual(t, -1, start, "the admin prompt must exist")
	drop := regexp.MustCompile(`(?m)^\s*admin = null;`).FindStringIndex(body[start:])
	require.NotNil(t, drop, "the drop must exist and must follow the prompt")
	span := body[start : start+drop[1]]

	for _, m := range assignment.FindAllStringSubmatch(span, -1) {
		target, value := m[1], quoted.ReplaceAllString(m[2], "")
		if target == "admin" {
			continue // assigning to admin itself is the trim, and the drop
		}
		// The request itself is the one destination the token is allowed to
		// reach: `var pending = fetch(... "Bearer " + admin ...)` binds a
		// promise, not the token. Every other assignment binds whatever
		// `admin` evaluates to, which is the copy this test is about.
		if strings.HasPrefix(strings.TrimSpace(value), "fetch(") {
			continue
		}
		require.False(t, adminIdentifier.MatchString(value),
			"the prompted admin token is copied out of `admin` by `%s =%s`: "+
				"between the prompt and the drop its value may reach the request and nothing else",
			target, m[2])
	}
}

// TestDashboardShipsNoTokenLiteral guards the served asset against a
// credential ever being baked into it — the admin token above all, since it
// clears devices fleet-wide, but the worker and client tokens too. The page
// is served to anyone who can reach the port, with no token at all.
func TestDashboardShipsNoTokenLiteral(t *testing.T) {
	body := dashboardBody(t)

	// The exact tokens newServer configures. If the page ever embedded the
	// admin token it would be this string.
	for _, tok := range []string{"atok", "ctok", "wtok"} {
		require.NotContains(t, body, tok)
	}
}

func TestUnknownPathIs404NotTheDashboard(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp, err := ts.Client().Get(ts.URL + "/nope")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// A non-GET request to an unknown path is 405, not 404: "GET /" is
// registered as a subtree pattern (it ends in "/"), so net/http's ServeMux
// matches the path for any method and then reports 405 itself once it sees
// no method matches — handleDashboard's own r.URL.Path != "/" check never
// even runs. That is standard library behaviour, not a bug, and it is
// defensible (a 405 correctly tells the caller the path exists under a
// different method rather than not at all) — this test exists so that
// behaviour is a pinned decision rather than an accident nobody would
// notice changing.
func TestUnknownPathNonGetIs405(t *testing.T) {
	ts, _, _, _ := newServer(t)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/nope", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}
