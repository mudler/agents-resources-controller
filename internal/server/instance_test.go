package server_test

import (
	"net/http"
	"testing"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// A worker has no other way to notice that the controller it is talking to
// is not the process it registered with. The controller's own restart is
// invisible from outside: the address is the same, the database is the same,
// and a restart short enough not to fail a single request is invisible
// entirely — which is exactly the restart that happened on 2026-08-18 and
// left a device quarantined with nobody left to prove it clean.
//
// So every response names the process that wrote it, and a worker that sees
// that name change knows to register again. It is an identity, not a
// credential: it says nothing about the fleet, gates nothing, and is stamped
// on unauthenticated responses too — a worker whose token was rotated out
// still deserves to know it is talking to a new controller.

func TestEveryResponseNamesTheControllerProcess(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register", registerBody(model.RecoveryProof{}))
	defer resp.Body.Close()
	instance := resp.Header.Get(server.ControllerInstanceHeader)
	require.NotEmpty(t, instance, "a worker cannot detect a restart it is never told about")

	// Stable for the life of the process: a value that changed per request
	// would have every worker re-registering on every poll.
	for _, path := range []string{"/v1/devices", "/v1/state"} {
		resp := get(t, ts, "ctok", path)
		require.Equal(t, instance, resp.Header.Get(server.ControllerInstanceHeader), path)
		resp.Body.Close()
	}

	// Including on a rejection: an unauthorised worker is still entitled to
	// know which controller rejected it.
	resp = get(t, ts, "nosuchtoken", "/v1/devices")
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, instance, resp.Header.Get(server.ControllerInstanceHeader))
}

// The property the whole reconnection path rests on: a restarted controller
// is a different process and must say so, even though it serves the same
// database at the same address.
func TestARestartedControllerReportsADifferentInstance(t *testing.T) {
	first, _, _, _ := newServer(t)
	second, _, _, _ := newServer(t)

	a := get(t, first, "ctok", "/v1/devices")
	defer a.Body.Close()
	b := get(t, second, "ctok", "/v1/devices")
	defer b.Body.Close()

	require.NotEqual(t,
		a.Header.Get(server.ControllerInstanceHeader),
		b.Header.Get(server.ControllerInstanceHeader),
		"two controller processes must not be indistinguishable to a worker")
}
