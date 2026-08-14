package server

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard/index.html
var dashboardHTML []byte

// handleDashboard serves the read-only dashboard. It carries no credential:
// the page prompts for a token and calls the API with it, so serving the
// page itself reveals nothing.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(dashboardHTML)
}
