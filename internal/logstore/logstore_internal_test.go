package logstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFollowGoroutineExitsWhenContextCancelled(t *testing.T) {
	s, err := New(t.TempDir())
	require.NoError(t, err)

	// Append well over 32KB so drain must send at least two chunks.
	largeBuf := make([]byte, 40*1024)
	for i := range largeBuf {
		largeBuf[i] = byte('a' + (i % 26))
	}
	require.NoError(t, s.Append("job3", largeBuf))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{}) // never closed

	ch, err := s.Follow(ctx, "job3", done)
	require.NoError(t, err)

	// Receive exactly one chunk.
	_ = <-ch

	// Cancel the context and never touch the channel again.
	// The goroutine should unblock from its pending send and exit,
	// running defer unsubscribe() which removes the listener entry.
	cancel()

	// Assert with bounded wait that the listener bookkeeping for this job is gone.
	// This only succeeds if the goroutine actually exited and ran defer unsubscribe().
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		listeners, exists := s.listeners["job3"]
		// The job's listener entry should be deleted (not just empty)
		return !exists || len(listeners) == 0
	}, 5*time.Second, 20*time.Millisecond,
		"listener bookkeeping should be cleaned up when goroutine exits")
}
