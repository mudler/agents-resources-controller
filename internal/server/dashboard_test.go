package server_test

import (
	"io"
	"net/http"
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
