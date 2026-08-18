// Package server exposes the controller's HTTP API. Every route is plain
// HTTP so it passes through the tunnels that already carry ssh.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/notify"
	"github.com/mudler/resource-controller/internal/store"
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
	// Notifier delivers operational events (a tripped watchdog, a failed
	// verify probe) to whatever sink the operator configured. A nil
	// Notifier is the supported and default state — no webhook configured —
	// and needs no special handling anywhere: every notify.Notifier method
	// is safe on a nil receiver, so emit below simply does nothing.
	Notifier *notify.Notifier
}

// ControllerInstanceHeader carries an identity for the controller PROCESS on
// every response it writes. A worker that sees it change knows the controller
// it registered with is gone and a new one is answering, which is the one
// thing it cannot otherwise detect: the address is the same, the database is
// the same, and a restart quick enough not to fail a request in flight is
// invisible from outside.
//
// That blind spot is half of the defect of 2026-08-18. Autonomous recovery
// evaluates a worker's proof at REGISTRATION, so a worker restart clears a
// recoverable quarantine and a CONTROLLER restart clears nothing: the worker
// never re-registered, never presented proof, and the device sat out of the
// pool until a human cleared it by hand. Seeing this value change is what
// sends the worker back through registration (see worker.contactTracker).
//
// It is an identity, not a credential. It grants nothing, says nothing about
// the fleet, and is deliberately stamped on every response including
// rejections -- a worker whose token was rotated out is still entitled to
// know it is talking to a new controller.
const ControllerInstanceHeader = "Rc-Controller-Instance"

type Server struct {
	cfg    Config
	notify *notifier
	events *broadcaster
	// instanceID identifies this process, and is generated per Server rather
	// than derived from anything durable (the host name, the database path)
	// on purpose: two processes serving the same database from the same host
	// across a restart are exactly the case a worker must be able to tell
	// apart.
	instanceID string
	// tty holds the live interactive sessions. In memory and deliberately
	// not persisted — see tty.go.
	tty *ttyRegistry
}

func New(cfg Config) *Server {
	return &Server{
		cfg:        cfg,
		notify:     newNotifier(),
		events:     newBroadcaster(),
		tty:        newTTYRegistry(),
		instanceID: uuid.NewString(),
	}
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

	// The two halves of an interactive session the worker dials out for. Both
	// are worker routes: a client token that could open them would be able to
	// inject output into somebody else's terminal and read their keystrokes.
	mux.Handle("POST /v1/jobs/{id}/tty/out", s.require("worker", s.handleTTYWorkerOut))
	mux.Handle("GET /v1/jobs/{id}/tty/in", s.require("worker", s.handleTTYWorkerIn))

	mux.Handle("POST /v1/jobs", s.require("client", s.handleSubmit))
	mux.Handle("GET /v1/jobs/{id}", s.require("client", s.handleGetJob))
	mux.Handle("POST /v1/jobs/{id}/kill", s.require("client", s.handleKill))
	mux.Handle("GET /v1/jobs/{id}/logs", s.require("client", s.handleStreamLogs))
	mux.Handle("GET /v1/jobs/{id}/tty/out", s.require("client", s.handleTTYClientOut))
	mux.Handle("POST /v1/jobs/{id}/tty/in", s.require("client", s.handleTTYClientIn))
	mux.Handle("GET /v1/devices", s.require("client", s.handleDevices))
	mux.Handle("GET /v1/devices/{id}/describe", s.require("client", s.handleDescribe))
	mux.Handle("GET /v1/explain", s.require("client", s.handleExplain))
	mux.Handle("GET /v1/state", s.require("client", s.handleState))
	mux.Handle("GET /v1/events", s.require("client", s.handleEvents))

	// whoami lets a caller find out what its own token can do. The dashboard
	// uses it to tell an admin session from a client one, so it can enable
	// admin controls for a token that has them instead of prompting for a
	// second token on every click. It reports the role and nothing else —
	// never the token, and it grants nothing.
	mux.Handle("GET /v1/whoami", s.require("client", s.handleWhoami))

	mux.Handle("POST /v1/devices/{id}/clear", s.require("admin", s.handleClearDevice))
	mux.Handle("DELETE /v1/devices/{id}", s.require("admin", s.handleRetireDevice))

	// Registered last so it cannot shadow any API route: ServeMux prefers
	// the most specific pattern regardless of registration order, but "/"
	// is the least specific pattern there is, so this ordering is really
	// just documentation of that fact for the next reader.
	mux.HandleFunc("GET /", s.handleDashboard)

	// Stamped around the whole mux, so no route added later can forget to
	// identify the process answering it, and so it lands on responses no
	// handler wrote at all (a 404, an auth rejection). It has to be set
	// before the handler runs: once a handler has written its status line
	// the header map is no longer consulted.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ControllerInstanceHeader, s.instanceID)
		mux.ServeHTTP(w, r)
	})
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

// emit is the single point at which this package announces an operational
// event, so the "who emits what" mapping can be read off the handlers rather
// than reconstructed from scattered notifier calls. It never blocks and
// never fails: notify.Notify drops rather than waits, which is what lets it
// be called from a request handler without a webhook outage ever becoming a
// scheduling outage.
//
// It is called emit, not notify, because this Server already has a field of
// that name — the long-poll waker in notify.go, an unrelated mechanism — and
// a struct cannot carry a field and a method with the same name. Renaming
// the older one was the alternative; leaving it alone keeps this task's diff
// to the wiring it is actually about.
//
// Event.At is deliberately not stamped here: notify.Notify fills it in when
// it is zero, and doing it twice would mean two sources of truth for the one
// field a consumer may sort or dedupe on.
func (s *Server) emit(e notify.Event) { s.cfg.Notifier.Notify(e) }

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
