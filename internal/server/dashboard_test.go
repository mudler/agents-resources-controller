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
