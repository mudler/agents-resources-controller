package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCpArgSplitsDeviceFromPath(t *testing.T) {
	remote, device, path, err := parseCpArg("dgx:gpu0:/workspace/train.py")
	require.NoError(t, err)
	require.True(t, remote)
	require.Equal(t, "dgx:gpu0", device, "a device ID contains a colon; only the LAST one separates the path")
	require.Equal(t, "/workspace/train.py", path)
}

func TestParseCpArgTreatsAPlainPathAsLocal(t *testing.T) {
	remote, _, path, err := parseCpArg("./train.py")
	require.NoError(t, err)
	require.False(t, remote)
	require.Equal(t, "./train.py", path)
}

// A Windows-style or relative path with a colon must not be mistaken for a
// device. Getting this wrong sends a local file to a device that does not
// exist, which at least fails — the reverse would silently write locally.
func TestParseCpArgRejectsAnAmbiguousRemote(t *testing.T) {
	_, _, _, err := parseCpArg("gpu0:/workspace/x")
	require.Error(t, err, "a device ID is host:name — one colon is not enough to name a device")
	require.Contains(t, err.Error(), "host:name:path")
}

func TestParseCpArgRejectsAnEmptyRemotePath(t *testing.T) {
	_, _, _, err := parseCpArg("dgx:gpu0:")
	require.Error(t, err)
}
