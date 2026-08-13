package cli_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func TestRenderDevicesShowsHolderElapsedAndStaleness(t *testing.T) {
	var out bytes.Buffer

	cli.RenderDevices(&out, []server.DeviceView{
		{
			Device:              model.Device{ID: "gpubox:gpu0", State: model.DeviceBusy, LastHeartbeatAt: time.Now()},
			Holder:              "mudler@laptop/sess-1",
			Command:             []string{"./bench", "--fast"},
			ElapsedSeconds:      125,
			HeartbeatAgeSeconds: 3,
		},
		{
			Device:              model.Device{ID: "gpubox:gpu1", State: model.DeviceUnknown},
			HeartbeatAgeSeconds: 47,
		},
	})

	got := out.String()
	require.Contains(t, got, "gpubox:gpu0")
	require.Contains(t, got, "busy")
	require.Contains(t, got, "mudler@laptop/sess-1")
	require.Contains(t, got, "2m5s")
	require.Contains(t, got, "./bench --fast")
	// The signal flock cannot express: this worker has gone quiet.
	require.Contains(t, got, "no contact 47s")
}
