package server

import "net/http"

// TEMPORARY — Task 5 scope. Handler() (server.go) already wires the client
// and admin routes so the mux is complete, but their real implementations
// belong to Task 6 (client API). These stubs exist only so the package
// compiles and the worker-API tests can run in isolation; they must be
// replaced wholesale by Task 6, not built upon.

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	writeErr(w, http.StatusNotImplemented, "not_implemented", "job submission ships in Task 6")
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	writeErr(w, http.StatusNotImplemented, "not_implemented", "job lookup ships in Task 6")
}

func (s *Server) handleStreamLogs(w http.ResponseWriter, r *http.Request) {
	writeErr(w, http.StatusNotImplemented, "not_implemented", "log streaming ships in Task 6")
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	writeErr(w, http.StatusNotImplemented, "not_implemented", "device listing ships in Task 6")
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeErr(w, http.StatusNotImplemented, "not_implemented", "state snapshot ships in Task 6")
}

func (s *Server) handleClearDevice(w http.ResponseWriter, r *http.Request) {
	writeErr(w, http.StatusNotImplemented, "not_implemented", "device clear ships in Task 6")
}
