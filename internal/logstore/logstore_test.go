package logstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/logstore"
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
