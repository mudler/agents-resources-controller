package worker_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// The boot ID must be stable within a boot: two reads return the same value.
// It is allowed to be empty on a platform that cannot provide one, which the
// controller treats as "no proof of a reboot".
func TestBootIDIsStableWithinABoot(t *testing.T) {
	first := worker.BootID()
	second := worker.BootID()
	require.Equal(t, first, second)
	if first != "" {
		require.NotContains(t, first, "\n", "boot id must be trimmed")
	}
}
