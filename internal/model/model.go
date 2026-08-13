// Package model holds the entities shared by the controller, worker, and client.
package model

import "time"

type DeviceState string

const (
	DeviceReady     DeviceState = "ready"
	DeviceBusy      DeviceState = "busy"
	DeviceUnknown   DeviceState = "unknown"
	DeviceUnhealthy DeviceState = "unhealthy"
)

type JobState string

const (
	JobAssigned  JobState = "assigned"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"
	JobKilled    JobState = "killed"
	JobLost      JobState = "lost"
	JobQueued    JobState = "queued"
)

// Terminal reports whether the job will never change state again.
func (s JobState) Terminal() bool {
	switch s {
	case JobSucceeded, JobFailed, JobKilled, JobLost:
		return true
	}
	return false
}

type Device struct {
	ID                string      `json:"id"` // "host:name"
	Host              string      `json:"host"`
	Name              string      `json:"name"`
	WorkerID          string      `json:"worker_id"`
	State             DeviceState `json:"state"`
	MaxRuntimeSeconds int         `json:"max_runtime_seconds,omitempty"`
	LastHeartbeatAt   time.Time   `json:"last_heartbeat_at"`
}

type Lease struct {
	ID         string    `json:"id"`
	DeviceID   string    `json:"device_id"`
	Holder     string    `json:"holder"`
	JobID      string    `json:"job_id,omitempty"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type Job struct {
	ID                 string            `json:"id"`
	Selector           string            `json:"selector"`
	Command            []string          `json:"command"`
	Cwd                string            `json:"cwd"`
	Env                map[string]string `json:"env,omitempty"`
	Submitter          string            `json:"submitter"`
	IdempotencyKey     string            `json:"idempotency_key,omitempty"`
	Priority           int               `json:"priority"`
	MaxRuntimeSeconds  int               `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int               `json:"idle_timeout_seconds,omitempty"`
	QueuedAt           time.Time         `json:"queued_at,omitempty"`
	State              JobState          `json:"state"`
	DeviceID           string            `json:"device_id"`
	WorkerID           string            `json:"worker_id"`
	ExitCode           *int              `json:"exit_code,omitempty"`
	KillReason         string            `json:"kill_reason,omitempty"`
	SubmittedAt        time.Time         `json:"submitted_at"`
	StartedAt          *time.Time        `json:"started_at,omitempty"`
	FinishedAt         *time.Time        `json:"finished_at,omitempty"`
}

type Worker struct {
	ID              string    `json:"id"`
	Host            string    `json:"host"`
	BootID          string    `json:"boot_id,omitempty"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}
