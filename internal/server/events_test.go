package server_test

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventStreamDeliversPublishedEvents(t *testing.T) {
	ts, _, _, _ := newServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", strings.Split(resp.Header.Get("Content-Type"), ";")[0])

	// Registering a worker publishes a state change.
	go func() {
		time.Sleep(100 * time.Millisecond)
		r := post(t, ts, "wtok", "/v1/workers/register", map[string]any{
			"host": "gpubox", "devices": []map[string]any{{"name": "gpu0"}},
		})
		r.Body.Close()
	}()

	sc := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !sc.Scan() {
			break
		}
		if strings.HasPrefix(sc.Text(), "data:") {
			require.Contains(t, sc.Text(), "devices")
			return
		}
	}
	t.Fatal("no event received within 5s")
}

// EventSource cannot send an Authorization header, so /v1/events alone also
// accepts the token as a query parameter.
func TestEventStreamAcceptsTokenAsQueryParameter(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp, err := ts.Client().Get(ts.URL + "/v1/events?token=ctok")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// That fallback must not leak to any other route: a token in a query string
// can land in access logs, so every other route keeps the header as the only
// accepted form.
func TestStateRejectsTokenAsQueryParameter(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp, err := ts.Client().Get(ts.URL + "/v1/state?token=ctok")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// The query-param fallback is confined to GET /v1/events. A POST route
// (client-scoped) must reject a query-string token exactly like it always
// rejected a missing header — the fallback in require() checks r.URL.Path,
// not the method, but this pins that a write route was never in scope.
func TestSubmitRejectsTokenAsQueryParameter(t *testing.T) {
	ts, _, _, _ := newServer(t)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/jobs?token=ctok",
		strings.NewReader(`{}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// Same confinement check for an admin route: an admin token in the query
// string must not authenticate the one manual-override endpoint that exists
// for a stuck GPU.
func TestClearDeviceRejectsTokenAsQueryParameter(t *testing.T) {
	ts, _, _, _ := newServer(t)

	req, err := http.NewRequest(http.MethodPost,
		ts.URL+"/v1/devices/some-device/clear?token=atok", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// A slow reader must not wedge the server.
func TestSlowEventReaderIsDroppedNotBlocking(t *testing.T) {
	ts, _, _, _ := newServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	// Never read from resp.Body; just publish a lot and make sure the server
	// stays responsive.
	defer resp.Body.Close()

	for i := 0; i < 500; i++ {
		r := post(t, ts, "wtok", "/v1/workers/register", map[string]any{
			"host": "gpubox", "devices": []map[string]any{{"name": "gpu0"}},
		})
		r.Body.Close()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r := get(t, ts, "ctok", "/v1/state")
		r.Body.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("server blocked behind a slow event reader")
	}
}
