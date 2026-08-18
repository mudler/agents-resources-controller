package cli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The example an error prints must itself be valid. This one interpolated the
// path straight after the device name and produced "dgx:gpu0/workspace/" —
// the very shape it was rejecting — so a reader who copied the suggestion got
// the same error back.
func TestCpErrorExampleIsItselfAcceptable(t *testing.T) {
	_, _, _, err := parseCpArg("gpu0:/workspace/")
	require.Error(t, err)

	i := strings.Index(err.Error(), "e.g. ")
	require.Greater(t, i, -1, "the error should carry an example")
	example := strings.TrimSuffix(strings.TrimSpace(err.Error()[i+len("e.g. "):]), ")")

	remote, device, path, err2 := parseCpArg(example)
	require.NoError(t, err2, "the example %q must parse", example)
	require.True(t, remote, "the example %q must be a remote path", example)
	require.NotEmpty(t, device)
	require.NotEmpty(t, path)
}
