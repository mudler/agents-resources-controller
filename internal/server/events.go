package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
)

// eventWriteTimeout bounds every write to an SSE connection. Without a
// deadline, a wedged peer (a suspended laptop, a blackholed route) blocks
// the handler goroutine inside a write forever: r.Context().Done() cannot
// preempt a write already in flight, so the deferred unsubscribe() never
// runs and the subscriber leaks — its channel entry, its goroutine, its
// fd — for as long as the process runs. This is the same leak shape
// internal/logstore and the worker notifier both had to fix once already.
const eventWriteTimeout = 10 * time.Second

type Event struct {
	Kind    string    `json:"kind"`
	At      time.Time `json:"at"`
	Payload any       `json:"payload,omitempty"`
}

type broadcaster struct {
	mu          sync.Mutex
	subscribers map[chan Event]struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{subscribers: map[chan Event]struct{}{}}
}

func (b *broadcaster) subscribe() (chan Event, func()) {
	// Buffered: a subscriber that stops reading loses events rather than
	// blocking every publisher behind it.
	ch := make(chan Event, 32)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		b.mu.Unlock()
	}
}

func (b *broadcaster) publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- e:
		default: // slow reader: drop rather than block the controller
		}
	}
}

// hasSubscribers reports whether any dashboard is currently connected.
// Callers whose payload is expensive to build (a store read, not just a
// struct literal) check this first so a controller nobody is watching
// does not pay for it.
func (b *broadcaster) hasSubscribers() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribers) > 0
}

// Publish emits an event to every connected dashboard. It never blocks.
func (s *Server) Publish(kind string, payload any) {
	s.events.publish(Event{Kind: kind, At: s.cfg.Clock.Now(), Payload: payload})
}

// jobEventPayload is the small "jobs" event body: just enough for a
// dashboard to know something changed and refetch /v1/state for the
// full picture.
type jobEventPayload struct {
	JobID string         `json:"job_id"`
	State model.JobState `json:"state"`
}

// publishJob emits a "jobs" event carrying a job's ID and its new state.
func (s *Server) publishJob(jobID string, state model.JobState) {
	s.Publish("jobs", jobEventPayload{JobID: jobID, State: state})
}

// publishDevices emits a "devices" event carrying the current device
// summary. Best-effort: if the store read fails, the transition that
// triggered this call already succeeded or already reported its own
// error, so a broken nudge here must not turn into a second error path —
// it is silently skipped, and the dashboard's own polling still catches
// up eventually.
//
// It skips the read entirely when nobody is subscribed: deviceViews()
// costs three store queries (Devices, Leases, AllLabels), and a controller
// running with no dashboard open — the common case — should not pay that
// on every registration and clear.
func (s *Server) publishDevices() {
	if !s.events.hasSubscribers() {
		return
	}
	views, err := s.deviceViews()
	if err != nil {
		return
	}
	s.Publish("devices", views)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if _, ok := w.(http.Flusher); !ok {
		writeErr(w, http.StatusInternalServerError, "unsupported", "streaming unsupported")
		return
	}
	rc := http.NewResponseController(w)

	ch, unsubscribe := s.events.subscribe()
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if !flushSSE(rc) {
		return
	}

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if !writeSSE(w, rc, ": keepalive\n\n") {
				return
			}
		case e := <-ch:
			body, err := json.Marshal(e)
			if err != nil {
				continue
			}
			if !writeSSE(w, rc, fmt.Sprintf("data: %s\n\n", body)) {
				return
			}
		}
	}
}

// writeSSE writes one SSE frame under eventWriteTimeout and flushes it,
// matching handleStreamLogs's sibling behaviour of returning (and so, via
// the caller's deferred unsubscribe, cleaning up) on any write error
// instead of looping forever on a connection that will never accept
// another byte. It reports whether the frame was delivered.
func writeSSE(w io.Writer, rc *http.ResponseController, frame string) bool {
	if err := rc.SetWriteDeadline(time.Now().Add(eventWriteTimeout)); err != nil {
		return false
	}
	if _, err := io.WriteString(w, frame); err != nil {
		return false
	}
	return flushSSE(rc)
}

// flushSSE flushes under the same bounded deadline as writeSSE, so the
// initial header flush (which has no frame body to write) gets the same
// wedged-connection protection as every event and keepalive that follows.
func flushSSE(rc *http.ResponseController) bool {
	if err := rc.SetWriteDeadline(time.Now().Add(eventWriteTimeout)); err != nil {
		return false
	}
	return rc.Flush() == nil
}
