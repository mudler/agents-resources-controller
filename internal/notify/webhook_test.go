package notify_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mudler/resource-controller/internal/notify"
	"github.com/stretchr/testify/require"
)

// TestWebhookPostsJSONWithContentType pins that the webhook sink posts the
// event body as JSON, tagged with the correct Content-Type. Deleting the
// header line (a mutation the reviewer confirmed survives without this
// test) would leave a webhook receiver unable to tell the body apart from
// an arbitrary POST.
func TestWebhookPostsJSONWithContentType(t *testing.T) {
	var gotContentType string
	var gotBody notify.Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := notify.NewWebhook(srv.URL, srv.Client())
	err := sink.Deliver(context.Background(), notify.Event{
		Kind:   notify.KindWatchdogTrip,
		Device: "gpubox:gpu0",
		Job:    "job-1",
		Reason: "no heartbeat",
	})
	require.NoError(t, err)
	require.Equal(t, "application/json", gotContentType)
	require.Equal(t, notify.KindWatchdogTrip, gotBody.Kind)
	require.Equal(t, "gpubox:gpu0", gotBody.Device)
}

// Any 2xx status, not just 200, must be treated as success.
func TestWebhook2xxIsSuccess(t *testing.T) {
	for _, status := range []int{201, 299} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			sink := notify.NewWebhook(srv.URL, srv.Client())
			err := sink.Deliver(context.Background(), notify.Event{Kind: notify.KindJobLost})
			require.NoError(t, err)
		})
	}
}

// Any non-2xx status must be treated as an error carrying the status code,
// so an operator's logs (via deliver's give-up warning) name what actually
// went wrong.
func TestWebhookNon2xxIsError(t *testing.T) {
	for _, status := range []int{400, 500} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			sink := notify.NewWebhook(srv.URL, srv.Client())
			err := sink.Deliver(context.Background(), notify.Event{Kind: notify.KindJobLost})
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("%d", status))
		})
	}
}

// An unread response body makes Go's transport discard the underlying
// connection instead of returning it to the idle pool: every subsequent
// delivery then pays a fresh TCP (and for HTTPS, TLS) handshake. This
// mirrors the reviewer's own measurement — 5 sequential deliveries against
// a keep-alive server should open exactly 1 TCP connection, not 5, which
// only happens if Deliver drains the body before closing it.
func TestWebhookDrainsResponseBodyForConnectionReuse(t *testing.T) {
	var newConns int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A non-empty body matters: draining an empty body reuses the
		// connection whether or not Deliver actually reads it, so this
		// wouldn't distinguish "drains the body" from "doesn't bother".
		w.Header().Set("Content-Length", "13")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello world!!"))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			atomic.AddInt32(&newConns, 1)
		}
	}
	srv.Start()
	defer srv.Close()

	sink := notify.NewWebhook(srv.URL, srv.Client())
	for i := 0; i < 5; i++ {
		require.NoError(t, sink.Deliver(context.Background(), notify.Event{Kind: notify.KindJobLost}))
	}

	require.EqualValues(t, 1, atomic.LoadInt32(&newConns),
		"an undrained response body defeats keep-alive; 5 deliveries should reuse one TCP connection, not open 5")
}

// captureTransport stands in for the network: it never dials anything, it
// just records whether the *http.Request it was handed carries a context
// deadline, then fabricates a 200 response. A context deadline is purely
// client-local bookkeeping (it never crosses the wire), so this is the only
// reliable way to observe what Deliver actually built — an httptest server
// inspecting its own r.Context() cannot see the client's request context at
// all, deadline or not.
type captureTransport struct {
	gotDeadline bool
}

func (c *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	_, c.gotDeadline = req.Context().Deadline()
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
}

// A *http.Client built as &http.Client{} (the zero value) has no timeout of
// its own. Deliver must still individually bound the request so a hanging
// endpoint cannot stall a delivery attempt forever; it does this by
// wrapping the outgoing request's context with context.WithTimeout. This
// asserts that mechanism directly, without waiting out the real bound or
// depending on a live server.
func TestWebhookBoundsRequestWhenClientHasNoTimeout(t *testing.T) {
	transport := &captureTransport{}
	client := &http.Client{Transport: transport} // zero Timeout: no timeout of its own

	sink := notify.NewWebhook("http://example.invalid/webhook", client)
	err := sink.Deliver(context.Background(), notify.Event{Kind: notify.KindJobLost})
	require.NoError(t, err)
	require.True(t, transport.gotDeadline,
		"a request built with a client that has no timeout must still carry a context deadline")
}
