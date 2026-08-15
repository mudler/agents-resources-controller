package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxDrainBytes bounds how much of a response body Deliver will read before
// closing it. The body's content is never used; this only exists so the
// underlying connection can be reused (see the comment on the drain below).
const maxDrainBytes = 64 << 10

// defaultWebhookTimeout bounds a webhook POST when the caller's *http.Client
// has no timeout of its own. Without this, a client built with
// &http.Client{} (the zero value, no timeout) would let a stuck endpoint
// hold a delivery attempt open indefinitely, defeating the retry budget's
// intent to give up and move on.
const defaultWebhookTimeout = 10 * time.Second

// webhookSink posts an Event as JSON to a fixed URL.
type webhookSink struct {
	url    string
	client *http.Client
}

// NewWebhook returns a Sink that POSTs each Event as JSON to url. If client
// is nil, a default client with a bounded timeout is used. If client is
// non-nil but has no timeout configured, each request is still individually
// bounded by defaultWebhookTimeout so an unreachable or hanging endpoint
// cannot stall a delivery attempt forever.
func NewWebhook(url string, client *http.Client) Sink {
	if client == nil {
		client = &http.Client{Timeout: defaultWebhookTimeout}
	}
	return &webhookSink{url: url, client: client}
}

// Deliver implements Sink.
func (w *webhookSink) Deliver(ctx context.Context, e Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("notify: marshal event: %w", err)
	}

	if w.client.Timeout <= 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultWebhookTimeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("notify: build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("notify: post webhook: %w", err)
	}
	defer resp.Body.Close()
	// Drain the body before closing it, on both the success and error
	// paths. An unread body makes Go's transport discard the connection
	// instead of returning it to the idle pool, so every delivery would
	// otherwise pay a fresh TCP (and for HTTPS, TLS) handshake — costly for
	// a controller that notifies steadily, and it piles up TIME_WAIT
	// sockets against the webhook host. The response content itself is
	// never used, so this is discarded, bounded so a misbehaving endpoint
	// can't make a delivery attempt read forever.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("notify: webhook returned status %d", resp.StatusCode)
	}
	return nil
}
