package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
)

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
func (s *Server) publishDevices() {
	views, err := s.deviceViews()
	if err != nil {
		return
	}
	s.Publish("devices", views)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "unsupported", "streaming unsupported")
		return
	}

	ch, unsubscribe := s.events.subscribe()
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case e := <-ch:
			body, err := json.Marshal(e)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", body)
			flusher.Flush()
		}
	}
}
