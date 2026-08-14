package server

import (
	"testing"
	"time"
)

// TestBroadcasterPublishNeverBlocksOnFullSubscriber exercises the property
// the whole broadcaster design exists for, directly: publish must return
// immediately even when a subscriber's buffer is completely full and
// nothing is draining it.
//
// This is an in-package test (package server, not server_test) because it
// needs subscribe/publish directly. The end-to-end
// TestSlowEventReaderIsDroppedNotBlocking in events_test.go exercises the
// same property over a real HTTP connection, but loopback socket buffers
// plus the transport's own buffering absorb hundreds of small events
// without ever actually filling the subscriber channel, so a regression to
// a blocking publish would not fail that test. This test fills the buffer
// itself and proves the next publish still returns promptly.
func TestBroadcasterPublishNeverBlocksOnFullSubscriber(t *testing.T) {
	b := newBroadcaster()
	ch, unsubscribe := b.subscribe()
	defer unsubscribe()

	// Fill the subscriber's buffer without ever reading from ch.
	for i := 0; i < cap(ch); i++ {
		b.publish(Event{Kind: "fill"})
	}
	if len(ch) != cap(ch) {
		t.Fatalf("setup: expected the subscriber buffer full (%d), got %d", cap(ch), len(ch))
	}

	done := make(chan struct{})
	go func() {
		b.publish(Event{Kind: "overflow"}) // must drop this event, not block
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("publish blocked on a full subscriber channel: the non-blocking guarantee is broken")
	}
}
