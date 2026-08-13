package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/stretchr/testify/require"
)

func TestSubmitReturnsErrNoDeviceOn409(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "no_device_available", "message": "gpubox:gpu0 is not free",
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	_, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"},
	})
	require.ErrorIs(t, err, client.ErrNoDevice)
}

func TestSubmitSendsBearerToken(t *testing.T) {
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"id": "job1", "state": "assigned"})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	job, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"},
	})
	require.NoError(t, err)
	require.Equal(t, "job1", job.ID)
	require.Equal(t, "Bearer ctok", gotAuth)
}
