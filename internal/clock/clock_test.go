package clock_test

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/stretchr/testify/require"
)

func TestFakeClockAdvances(t *testing.T) {
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	c := clock.NewFake(base)

	require.Equal(t, base, c.Now())

	c.Advance(90 * time.Second)
	require.Equal(t, base.Add(90*time.Second), c.Now())
}
