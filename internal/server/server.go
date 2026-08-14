// Package server exposes the controller's HTTP API. Every route is plain
// HTTP so it passes through the tunnels that already carry ssh.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/store"
)

// contextKey is an unexported type so values this package stores on a
// request context can never collide with a key set by another package.
type contextKey int

const roleContextKey contextKey = iota

type Config struct {
	Store  *store.Store
	Logs   *logstore.Store
	Clock  clock.Clock
	Tokens map[string]string // token -> role: worker | client | admin
}

type Server struct {
	cfg    Config
	notify *notifier
	events *broadcaster
}

func New(cfg Config) *Server {
	return &Server{cfg: cfg, notify: newNotifier(), events: newBroadcaster()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("POST /v1/workers/register", s.require("worker", s.handleRegister))
	mux.Handle("POST /v1/workers/{id}/labels", s.require("worker", s.handlePushLabels))
	mux.Handle("GET /v1/workers/{id}/assignments", s.require("worker", s.handleAssignments))
	mux.Handle("POST /v1/workers/{id}/heartbeat", s.require("worker", s.handleHeartbeat))
	mux.Handle("POST /v1/jobs/{id}/logs", s.require("worker", s.handleAppendLogs))
	mux.Handle("POST /v1/jobs/{id}/status", s.require("worker", s.handleJobStatus))
	mux.Handle("POST /v1/devices/{id}/fault", s.require("worker", s.handleDeviceFault))

	mux.Handle("POST /v1/jobs", s.require("client", s.handleSubmit))
	mux.Handle("GET /v1/jobs/{id}", s.require("client", s.handleGetJob))
	mux.Handle("POST /v1/jobs/{id}/kill", s.require("client", s.handleKill))
	mux.Handle("GET /v1/jobs/{id}/logs", s.require("client", s.handleStreamLogs))
	mux.Handle("GET /v1/devices", s.require("client", s.handleDevices))
	mux.Handle("GET /v1/state", s.require("client", s.handleState))
	mux.Handle("GET /v1/events", s.require("client", s.handleEvents))

	mux.Handle("POST /v1/devices/{id}/clear", s.require("admin", s.handleClearDevice))

	// Registered last so it cannot shadow any API route: ServeMux prefers
	// the most specific pattern regardless of registration order, but "/"
	// is the least specific pattern there is, so this ordering is really
	// just documentation of that fact for the next reader.
	mux.HandleFunc("GET /", s.handleDashboard)

	return mux
}

// require authenticates the bearer token and enforces the minimum role.
// Roles are ordered: admin outranks client outranks worker for client routes,
// but worker routes accept only worker and admin tokens.
func (s *Server) require(role string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		token, hasScheme := strings.CutPrefix(auth, "Bearer ")
		// Browsers' EventSource cannot set an Authorization header, so the
		// event stream alone also accepts the token as a query parameter.
		// Nowhere else: a token in a query string can land in access logs
		// and proxy logs, so every other route keeps the header as the
		// only accepted form.
		if token == "" && r.URL.Path == "/v1/events" {
			token = r.URL.Query().Get("token")
			hasScheme = token != ""
		}
		got, ok := s.cfg.Tokens[token]
		if !hasScheme || token == "" || !ok {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "unknown or missing token")
			return
		}
		if !allows(got, role) {
			writeErr(w, http.StatusForbidden, "forbidden", "token role "+got+" may not call a "+role+" route")
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), roleContextKey, got))
		h(w, r)
	})
}

func allows(have, want string) bool {
	if have == "admin" || have == want {
		return true
	}
	return false
}

// isAdmin reports whether the token that authenticated this request resolved
// to the admin role. require stores that role on the request context, so a
// handler that needs to distinguish "the owner" from "an operator overriding
// on someone else's behalf" can read it here rather than re-deriving it.
func isAdmin(r *http.Request) bool {
	role, _ := r.Context().Value(roleContextKey).(string)
	return role == "admin"
}

// Poke wakes a worker's assignment long-poll immediately. Exported so callers
// outside this package (the scheduler loop in `rc serve`) can nudge a worker
// without reaching into unexported state.
func (s *Server) Poke(workerID string) { s.notify.poke(workerID) }

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: code, Message: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "err", err)
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	return true
}
