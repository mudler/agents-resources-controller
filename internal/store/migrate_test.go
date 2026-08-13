package store_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// A database created by Stage 1 must gain Stage 2's columns on open, not
// silently keep the old shape.
func TestOpenMigratesAStageOneDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rc.db")

	// Build a Stage 1 database by hand: the old tables, no user_version.
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`
		CREATE TABLE workers (id TEXT PRIMARY KEY, host TEXT NOT NULL, last_heartbeat_at INTEGER NOT NULL);
		CREATE TABLE devices (id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT NOT NULL,
		  worker_id TEXT NOT NULL, state TEXT NOT NULL, last_heartbeat_at INTEGER NOT NULL);
		CREATE TABLE jobs (id TEXT PRIMARY KEY, selector TEXT NOT NULL DEFAULT '', command TEXT NOT NULL,
		  cwd TEXT NOT NULL DEFAULT '', env TEXT NOT NULL DEFAULT '{}', submitter TEXT NOT NULL DEFAULT '',
		  idempotency_key TEXT, state TEXT NOT NULL, device_id TEXT NOT NULL DEFAULT '',
		  worker_id TEXT NOT NULL DEFAULT '', exit_code INTEGER, kill_reason TEXT NOT NULL DEFAULT '',
		  submitted_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER);
		CREATE TABLE leases (id TEXT PRIMARY KEY, device_id TEXT NOT NULL, holder TEXT NOT NULL,
		  job_id TEXT NOT NULL DEFAULT '', acquired_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
		  released_at INTEGER);
		INSERT INTO workers VALUES ('w1', 'gpubox', 100);
	`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))
	s, err := store.Open(path, c)
	require.NoError(t, err)
	defer s.Close()

	check, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer check.Close()

	for _, col := range []struct{ table, column string }{
		{"jobs", "priority"},
		{"jobs", "max_runtime"},
		{"jobs", "idle_timeout"},
		{"jobs", "queued_at"},
		{"devices", "max_runtime"},
		{"workers", "boot_id"},
	} {
		var count int
		require.NoError(t, check.QueryRow(
			`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, col.table, col.column,
		).Scan(&count), "%s.%s", col.table, col.column)
		require.Equal(t, 1, count, "%s.%s missing after migration", col.table, col.column)
	}

	var tables int
	require.NoError(t, check.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='reservations'`).Scan(&tables))
	require.Equal(t, 1, tables, "reservations table missing")

	// Pre-existing rows survive.
	var host string
	require.NoError(t, check.QueryRow(`SELECT host FROM workers WHERE id='w1'`).Scan(&host))
	require.Equal(t, "gpubox", host)
}

func TestMigrationsAreIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rc.db")
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))

	for i := 0; i < 3; i++ {
		s, err := store.Open(path, c)
		require.NoError(t, err, "open #%d", i)
		require.NoError(t, s.Close())
	}

	check, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer check.Close()

	var version int
	require.NoError(t, check.QueryRow(`PRAGMA user_version`).Scan(&version))
	require.Greater(t, version, 0, "user_version should record applied migrations")
}
