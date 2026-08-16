package store

import (
	"database/sql"
	"errors"
	"fmt"
)

// ErrDeviceBusy is returned when an operation needs a device nobody is
// holding and something holds it.
var ErrDeviceBusy = errors.New("device has a live lease")

// RetireDevice removes a device from the fleet: the card was pulled, or the
// box was decommissioned. It is the counterpart to a worker registering one,
// and it exists because the alternative was editing the database by hand.
//
// What it does NOT delete is job history. Jobs carry device_id as plain text
// with no foreign key precisely so the record of what ran where outlives the
// hardware — "which box ran this, and how did it go" is asked most often
// about a box that is no longer there.
//
// It refuses while anything holds the device. Deleting the row under a live
// lease would strand a running job on hardware the controller no longer
// believes exists, and the lease is the one guarantee this system makes.
//
// A caveat that belongs to the operator, not to this function: if the
// device's worker is still running and still declares the device, the next
// registration recreates it. Remove it from the worker's config first, or
// stop the worker. This is deliberately not enforced here — the ordinary
// case is dropping a device from worker.yaml on a host that is otherwise
// still in service, and refusing whenever the worker was alive would block
// exactly that.
func (s *Store) RetireDevice(id string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var state string
	if err := tx.QueryRow(`SELECT state FROM devices WHERE id = ?`, id).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrDeviceNotFound, id)
		}
		return err
	}

	var live int
	if err := tx.QueryRow(
		`SELECT COUNT(*) FROM leases WHERE device_id = ? AND released_at IS NULL`, id,
	).Scan(&live); err != nil {
		return err
	}
	if live > 0 {
		return fmt.Errorf("%w: %s", ErrDeviceBusy, id)
	}

	// Everything keyed to this device goes with it. Each of these is a
	// separate statement rather than a cascade because there are no foreign
	// keys between them: device_id is plain text everywhere it appears, so
	// nothing is deleted implicitly and the list of what dies with a device
	// is exactly what is written here.
	for _, stmt := range []string{
		`DELETE FROM device_labels WHERE device_id = ?`,
		`DELETE FROM host_docs WHERE device_id = ?`,
		`DELETE FROM reservations WHERE device_id = ?`,
		`DELETE FROM leases WHERE device_id = ?`,
		`DELETE FROM devices WHERE id = ?`,
	} {
		if _, err := tx.Exec(stmt, id); err != nil {
			return fmt.Errorf("retire %s: %w", id, err)
		}
	}

	return tx.Commit()
}
