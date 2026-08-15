package server

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard/index.html
var dashboardHTML []byte

// handleDashboard serves the dashboard. It carries no credential: the page
// prompts for a client token and calls the API with it, so serving the page
// itself reveals nothing. The one thing on the page that needs more than a
// client token — clearing an unhealthy device — prompts for an admin token
// per click and keeps it nowhere, so this asset stays safe to hand to
// anyone who can reach the port.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(dashboardHTML)
}
