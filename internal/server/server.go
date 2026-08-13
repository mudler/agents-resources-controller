// Package server exposes the controller's HTTP API. Every route is plain
// HTTP so it passes through the tunnels that already carry ssh.
package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/store"
)

type Config struct {
	Store  *store.Store
	Logs   *logstore.Store
	Clock  clock.Clock
	Tokens map[string]string // token -> role: worker | client | admin
}

type Server struct {
	cfg    Config
	notify *notifier
}

func New(cfg Config) *Server {
	return &Server{cfg: cfg, notify: newNotifier()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("POST /v1/workers/register", s.require("worker", s.handleRegister))
	mux.Handle("GET /v1/workers/{id}/assignments", s.require("worker", s.handleAssignments))
	mux.Handle("POST /v1/workers/{id}/heartbeat", s.require("worker", s.handleHeartbeat))
	mux.Handle("POST /v1/jobs/{id}/logs", s.require("worker", s.handleAppendLogs))
	mux.Handle("POST /v1/jobs/{id}/status", s.require("worker", s.handleJobStatus))

	mux.Handle("POST /v1/jobs", s.require("client", s.handleSubmit))
	mux.Handle("GET /v1/jobs/{id}", s.require("client", s.handleGetJob))
	mux.Handle("GET /v1/jobs/{id}/logs", s.require("client", s.handleStreamLogs))
	mux.Handle("GET /v1/devices", s.require("client", s.handleDevices))
	mux.Handle("GET /v1/state", s.require("client", s.handleState))

	mux.Handle("POST /v1/devices/{id}/clear", s.require("admin", s.handleClearDevice))

	return mux
}

// require authenticates the bearer token and enforces the minimum role.
// Roles are ordered: admin outranks client outranks worker for client routes,
// but worker routes accept only worker and admin tokens.
func (s *Server) require(role string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		token, hasScheme := strings.CutPrefix(auth, "Bearer ")
		got, ok := s.cfg.Tokens[token]
		if !hasScheme || token == "" || !ok {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "unknown or missing token")
			return
		}
		if !allows(got, role) {
			writeErr(w, http.StatusForbidden, "forbidden", "token role "+got+" may not call a "+role+" route")
			return
		}
		h(w, r)
	})
}

func allows(have, want string) bool {
	if have == "admin" || have == want {
		return true
	}
	return false
}

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
