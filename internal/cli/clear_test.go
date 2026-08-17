package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClearPostsToTheClearEndpoint(t *testing.T) {
	var gotPath, gotMethod, gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod, gotAuth = r.URL.Path, r.Method, r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "admintok")

	cmd := NewClearCmd()
	cmd.SetArgs([]string{"dgx:gpu0"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	require.NoError(t, cmd.Execute())

	// The device ID contains a colon, which must survive path escaping —
	// getting this wrong clears nothing and reports success.
	require.Equal(t, "/v1/devices/dgx:gpu0/clear", gotPath)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "Bearer admintok", gotAuth)
	require.Contains(t, out.String(), "dgx:gpu0")
}

// A device with a live lease is refused by the store, and the API answers 409.
// Reporting success there would tell an operator the opposite of what happened
// on the one manual override that exists for a stuck GPU.
func TestClearReportsARefusedClear(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"device_not_cleared","message":"device has a live lease and was not cleared"}`))
	}))
	defer ts.Close()
	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "admintok")

	cmd := NewClearCmd()
	cmd.SetArgs([]string{"dgx:gpu0"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "live lease")
}

func TestClearRequiresADevice(t *testing.T) {
	cmd := NewClearCmd()
	cmd.SetArgs([]string{})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	require.Error(t, cmd.Execute())
}
