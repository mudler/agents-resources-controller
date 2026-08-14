package store

import (
	"database/sql"
	"errors"
	"time"
)

// UpsertHostDoc stores (or replaces) one usage-sheet document. deviceID ==
// "" is the host-wide sheet (applies to every device on that host in
// `rc describe`); any other value is one device's own sheet, keyed by its
// full "host:name" ID. A second call for the same (host, deviceID) replaces
// the body outright — the row is a live snapshot of whatever the worker's
// disk currently holds, not an append-only log, so a deleted sheet file
// legitimately overwrites a previous body with the empty string.
func (s *Store) UpsertHostDoc(host, deviceID, body string, at time.Time) error {
	_, err := s.db.Exec(
		`INSERT INTO host_docs (host, device_id, body, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(host, device_id) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at`,
		host, deviceID, body, at.Unix())
	return err
}

// HostDoc returns one usage-sheet document and when it was last written. A
// (host, deviceID) pair with no row yields an empty body and the zero time,
// not an error — most hosts, and most devices, will never have written one.
func (s *Store) HostDoc(host, deviceID string) (string, time.Time, error) {
	var body string
	var updated int64
	err := s.db.QueryRow(
		`SELECT body, updated_at FROM host_docs WHERE host = ? AND device_id = ?`,
		host, deviceID).Scan(&body, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, nil
	}
	if err != nil {
		return "", time.Time{}, err
	}
	return body, time.Unix(updated, 0).UTC(), nil
}
