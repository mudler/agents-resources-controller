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

const (
	SourceDetected = "detected"
	SourceDeclared = "declared"
)

const (
	LeaseKindJob  = "job"
	LeaseKindHold = "hold"
)

// A job's stdio mode: where the process's standard streams are wired.
//
// The default, StdioLogs, is what every job has always had — output batched
// up to the controller and kept in the log store, no input at all. The other
// two attach the process to the controller's in-memory relay instead
// (internal/server/tty.go), which is what makes a terminal and a file
// transfer possible without the controller ever storing a byte of either.
//
// The two attached modes differ in exactly one thing: whether the far end is
// a pseudo-terminal. That single difference decides both halves of the wire,
// so it is one field with three values rather than two booleans that could
// contradict each other:
//
//   - StdioTTY  — a PTY. Output is raw bytes; input is newline-delimited
//     JSON frames (server.TTYFrame), because it has to carry window resizes
//     as well as keystrokes.
//   - StdioPipe — ordinary pipes. Both directions are raw bytes with no
//     framing, because there is no terminal to resize and because `rc cp`
//     pushes a tar stream through this: base64-in-JSON would cost a third
//     more bytes over the wire for nothing. A PTY would be actively wrong
//     here — its line discipline rewrites \n, eats ^C and ^D, and would
//     corrupt any binary payload.
const (
	StdioLogs = ""
	StdioTTY  = "tty"
	StdioPipe = "pipe"
)

// StdioAttached reports whether a mode wires the process to the relay rather
// than to the log store.
func StdioAttached(mode string) bool { return mode == StdioTTY || mode == StdioPipe }

// Terminal reports whether the job will never change state again.
func (s JobState) Terminal() bool {
	switch s {
	case JobSucceeded, JobFailed, JobKilled, JobLost:
		return true
	}
	return false
}

// Label is one fact about a device, with where it came from and when it was
// last confirmed. Provenance matters because a hand-written value that
// survives a hardware change is worse than no value at all.
type Label struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Source    string    `json:"source"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Device struct {
	ID                string      `json:"id"` // "host:name"
	Host              string      `json:"host"`
	Name              string      `json:"name"`
	WorkerID          string      `json:"worker_id"`
	State             DeviceState `json:"state"`
	MaxRuntimeSeconds int         `json:"max_runtime_seconds,omitempty"`
	LastHeartbeatAt   time.Time   `json:"last_heartbeat_at"`
	Labels            []Label     `json:"labels,omitempty"`
}

type Lease struct {
	ID         string    `json:"id"`
	DeviceID   string    `json:"device_id"`
	Holder     string    `json:"holder"`
	JobID      string    `json:"job_id,omitempty"`
	Kind       string    `json:"kind"`
	Reason     string    `json:"reason,omitempty"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type Job struct {
	ID       string `json:"id"`
	Selector string `json:"selector"`
	// Kind is LeaseKindJob or LeaseKindHold. A hold is a job whose command
	// the worker chooses for itself (see internal/worker's execute) rather
	// than anything the submitter supplied — see server.handleSubmit, which
	// rejects a hold submission that carries one.
	Kind               string            `json:"kind"`
	Command            []string          `json:"command"`
	Cwd                string            `json:"cwd"`
	Env                map[string]string `json:"env,omitempty"`
	Submitter          string            `json:"submitter"`
	IdempotencyKey     string            `json:"idempotency_key,omitempty"`
	Priority           int               `json:"priority"`
	MaxRuntimeSeconds  int               `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int               `json:"idle_timeout_seconds,omitempty"`
	QueuedAt           *time.Time        `json:"queued_at,omitempty"`
	State              JobState          `json:"state"`
	DeviceID           string            `json:"device_id"`
	WorkerID           string            `json:"worker_id"`
	ExitCode           *int              `json:"exit_code,omitempty"`
	KillReason         string            `json:"kill_reason,omitempty"`
	// Reason is why a hold was taken (e.g. "manual profiling"), shown by
	// rc devices and the dashboard via the lease row it is copied onto at
	// assignment. Empty for an ordinary job.
	Reason string `json:"reason,omitempty"`
	// Stdio is StdioLogs, StdioTTY or StdioPipe — where this job's standard
	// streams are wired. The worker only ever learns a job is interactive
	// from the assignment this is copied onto, since the worker is the side
	// that dials out.
	Stdio       string     `json:"stdio,omitempty"`
	SubmittedAt time.Time  `json:"submitted_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

type Worker struct {
	ID              string    `json:"id"`
	Host            string    `json:"host"`
	BootID          string    `json:"boot_id,omitempty"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}
