package server

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/mudler/resource-controller/internal/store"
)

// WhoamiResponse tells a caller what its own token can do. It deliberately
// carries the role and nothing else: not the token, not the token list, not
// how many tokens exist. It grants nothing — every route still checks the
// role for itself — so this is a convenience for building a UI that matches
// the caller's actual powers, never a substitute for those checks.
type WhoamiResponse struct {
	Role string `json:"role"`
}

func (s *Server) handleWhoami(w http.ResponseWriter, r *http.Request) {
	role, _ := r.Context().Value(roleContextKey).(string)
	writeJSON(w, http.StatusOK, WhoamiResponse{Role: role})
}

// handleRetireDevice removes a device from the fleet for good — the card was
// pulled, the box was decommissioned. Admin-only, and refused while anything
// holds the device.
//
// Note what it cannot do on its own: if the device's worker is still running
// and still declares this device, the next registration recreates it. The
// response says so rather than leaving an operator to discover it when the
// row reappears thirty seconds later.
func (s *Server) handleRetireDevice(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.cfg.Store.RetireDevice(id)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeErr(w, http.StatusNotFound, "device_not_found", "no such device: "+id)
		return
	case errors.Is(err, store.ErrDeviceBusy):
		writeErr(w, http.StatusConflict, "device_busy",
			"something holds "+id+": kill or release it first, then retire")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	slog.Warn("device retired", "device", id)
	// The fleet just changed shape, so every dashboard watching the stream
	// redraws rather than waiting out its poll. publishDevices is the same
	// helper registration and clear use, and it costs nothing when no one is
	// subscribed.
	s.publishDevices()
	writeJSON(w, http.StatusOK, map[string]string{
		"retired": id,
		"note":    "if a worker still declares this device, it will reappear on its next registration — remove it from that worker's config",
	})
}
