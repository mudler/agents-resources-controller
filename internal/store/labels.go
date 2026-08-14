package store

import (
	"fmt"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
)

// ReplaceLabels makes the stored labels for one device and one source exactly
// the given set. Replacing rather than merging is deliberate: a probe that
// stops reporting a key means the fact is gone (a card was swapped, a driver
// downgraded), and a stale label is worse than a missing one.
func (s *Store) ReplaceLabels(deviceID, source string, labels map[string]string, at time.Time) error {
	if source != model.SourceDetected && source != model.SourceDeclared {
		return fmt.Errorf("unknown label source %q", source)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`DELETE FROM device_labels WHERE device_id = ? AND source = ?`, deviceID, source); err != nil {
		return fmt.Errorf("clear labels: %w", err)
	}
	for k, v := range labels {
		if _, err := tx.Exec(
			`INSERT INTO device_labels (device_id, key, value, source, updated_at)
			 VALUES (?, ?, ?, ?, ?)`,
			deviceID, k, v, source, at.Unix()); err != nil {
			return fmt.Errorf("insert label %q: %w", k, err)
		}
	}
	return tx.Commit()
}

// LabelsFor returns every stored row for a device, both sources, so a caller
// can show the conflict rather than hide it.
func (s *Store) LabelsFor(deviceID string) ([]model.Label, error) {
	rows, err := s.db.Query(
		`SELECT key, value, source, updated_at FROM device_labels
		 WHERE device_id = ? ORDER BY key, source`, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Label
	for rows.Next() {
		var l model.Label
		var updated int64
		if err := rows.Scan(&l.Key, &l.Value, &l.Source, &updated); err != nil {
			return nil, err
		}
		l.UpdatedAt = time.Unix(updated, 0).UTC()
		out = append(out, l)
	}
	return out, rows.Err()
}

// LabelSnapshot returns the effective labels the scheduler matches against:
// device ID -> key -> value, with a detected value winning over a declared
// one for the same key.
func (s *Store) LabelSnapshot() (map[string]map[string]string, error) {
	// Declared first, then detected, so detected overwrites in the map.
	rows, err := s.db.Query(
		`SELECT device_id, key, value FROM device_labels
		 ORDER BY device_id, key, CASE source WHEN ? THEN 0 ELSE 1 END`,
		model.SourceDeclared)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]map[string]string{}
	for rows.Next() {
		var device, key, value string
		if err := rows.Scan(&device, &key, &value); err != nil {
			return nil, err
		}
		if out[device] == nil {
			out[device] = map[string]string{}
		}
		out[device][key] = value
	}
	return out, rows.Err()
}
