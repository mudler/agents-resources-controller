CREATE TABLE IF NOT EXISTS workers (
  id                TEXT PRIMARY KEY,
  host              TEXT NOT NULL,
  last_heartbeat_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
  id                TEXT PRIMARY KEY,
  host              TEXT NOT NULL,
  name              TEXT NOT NULL,
  worker_id         TEXT NOT NULL,
  state             TEXT NOT NULL,
  last_heartbeat_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  id              TEXT PRIMARY KEY,
  selector        TEXT NOT NULL DEFAULT '',
  command         TEXT NOT NULL,
  cwd             TEXT NOT NULL DEFAULT '',
  env             TEXT NOT NULL DEFAULT '{}',
  submitter       TEXT NOT NULL DEFAULT '',
  idempotency_key TEXT,
  state           TEXT NOT NULL,
  device_id       TEXT NOT NULL DEFAULT '',
  worker_id       TEXT NOT NULL DEFAULT '',
  exit_code       INTEGER,
  kill_reason     TEXT NOT NULL DEFAULT '',
  submitted_at    INTEGER NOT NULL,
  started_at      INTEGER,
  finished_at     INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency
  ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS leases (
  id          TEXT PRIMARY KEY,
  device_id   TEXT NOT NULL,
  holder      TEXT NOT NULL,
  job_id      TEXT NOT NULL DEFAULT '',
  acquired_at INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,
  released_at INTEGER
);

-- The invariant, enforced by the database: at most one live lease per device.
CREATE UNIQUE INDEX IF NOT EXISTS leases_one_live_per_device
  ON leases(device_id) WHERE released_at IS NULL;
