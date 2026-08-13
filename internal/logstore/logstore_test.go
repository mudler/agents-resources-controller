package logstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/stretchr/testify/require"
)

func TestAppendThenRead(t *testing.T) {
	s, err := logstore.New(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, s.Append("job1", []byte("hello ")))
	require.NoError(t, s.Append("job1", []byte("world")))

	got, err := s.Read("job1")
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
}

func TestFollowDeliversExistingThenNewChunks(t *testing.T) {
	s, err := logstore.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, s.Append("job1", []byte("first\n")))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})

	ch, err := s.Follow(ctx, "job1", done)
	require.NoError(t, err)

	require.Equal(t, "first\n", string(<-ch))

	require.NoError(t, s.Append("job1", []byte("second\n")))
	require.Equal(t, "second\n", string(<-ch))

	close(done)
	for range ch { // drain until the follower closes the channel
	}
}

func TestFollowGoroutineExitsWhenContextCancelled(t *testing.T) {
	s, err := logstore.New(t.TempDir())
	require.NoError(t, err)

	// Write more than 32KB to ensure drain must send multiple chunks.
	largeBuf := make([]byte, 40*1024)
	for i := range largeBuf {
		largeBuf[i] = byte('a' + (i % 26))
	}
	require.NoError(t, s.Append("job2", largeBuf))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})

	ch, err := s.Follow(ctx, "job2", done)
	require.NoError(t, err)

	// Read exactly one chunk.
	_ = <-ch

	// Cancel the context without reading further or closing done.
	// The goroutine should exit and the channel should close.
	cancel()

	// Wait for the channel to close with a bounded timeout.
	// If the goroutine leaks, this will timeout and fail.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()

	closed := false
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
				break
			}
		case <-waitCtx.Done():
			t.Fatal("channel did not close within 5 seconds; goroutine likely leaked")
		}
		if closed {
			break
		}
	}
}
