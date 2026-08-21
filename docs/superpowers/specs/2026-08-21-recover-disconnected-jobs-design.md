# Optionally retain jobs after a worker disconnects

## Goal

Add an opt-in controller mode that keeps jobs recoverable while their worker is
unreachable. Let an operator remove queued or disconnected jobs with
`rc kill <job-id>`.

## Configuration

`rc serve` adds this controller-wide flag:

```text
--retain-disconnected-jobs
```

The flag defaults to `false`. The default preserves the current automatic
`lost` behavior. All workers connected to one controller use the same policy.

The controller reads the flag at startup. Changing the policy requires a
controller restart. The setting does not modify jobs that already reached a
terminal state.

## Current problem

The controller changes an `assigned` or `running` job to `lost` after its worker
misses heartbeats. The controller also releases the lease. The job then leaves
`rc ps`, which makes the job appear deleted.

Network loss does not prove that the job process stopped. A worker can continue
to supervise the process while it cannot reach the controller. The controller
must not convert this uncertainty into a terminal result when retention mode is
enabled.

## Job and device lifecycle

The controller does not add a cosmetic `paused` job state. It keeps the job in
its existing `assigned` or `running` state. This preserves compatibility with
clients and records the last state that the worker proved.

When `--retain-disconnected-jobs` is enabled, the sweep applies these rules
after a worker stops heartbeating:

- After the existing grace period, the controller changes the device to
  `unknown`.
- After the existing unhealthy period, the controller changes the device to
  `unhealthy` with reason `worker_lost`.
- The controller does not change the job to `lost` because of worker silence.
- The controller does not release or expire the job lease because of worker
  silence.
- The controller does not schedule another job on the device.

Job leases have no wall-clock expiry in this mode. Worker heartbeats can
continue to update their deadline for compatibility and diagnostics. The sweep
does not expire a lease whose kind is `job`. Non-job leases, including holds,
keep their current expiry behavior.

When the flag is disabled, the sweep keeps the current behavior. It marks jobs
`lost` and releases expired job leases after the current timeouts.

## Reconnection

A live worker includes its supervised job IDs in each heartbeat. When the worker
returns and names an active job, the controller changes a device quarantined for
`worker_lost` back to `busy`. The job keeps its current state and later finishes
through the normal terminal report.

The controller does not restore a device quarantined for `fault` or another
cause. A heartbeat proves worker and job liveness. It does not prove that a
reported hardware fault disappeared.

The worker already avoids re-registration while it supervises a job. This rule
remains. A new worker process has no supervised job to report. Its registration
reconciles old `assigned` or `running` jobs as `lost`, as it does now. A changed
boot ID also proves that the old process cannot continue.

## Manual cancellation

`rc kill <job-id>` remains the only removal command. The command does not delete
history. The disconnected-job rules apply when retention mode is enabled.

- For a `queued` job, the controller changes the state to `killed` and removes
  its queue reservation immediately. This is the current behavior.
- For a job on a reachable worker, the controller sends the kill request to the
  worker. The worker stops the process and sends the terminal report.
- For a job whose device is `unknown` or quarantined for `worker_lost`, the
  controller changes the state to `killed` and releases the lease immediately.
  It keeps the device out of the scheduling pool.
- The controller retains the kill request for delivery if the old worker and
  process return. This prevents an immediately finalized cancellation from
  allowing the remote process to continue unnoticed.

An operator can therefore remove a blocked job without waiting for the host.
The device remains unavailable until the worker returns with valid evidence or
an operator clears it through the existing recovery path.

## API and storage behavior

The existing `POST /v1/jobs/{id}/kill` endpoint and CLI syntax do not change.
The server passes the controller policy to the sweep and kill paths. The store
adds one atomic cancellation operation for active jobs. It reads the device
state and quarantine reason in the same transaction that updates the job and
lease.

The operation returns one of these outcomes:

- `requested`: a reachable worker must perform the kill.
- `finalized`: the controller finalized an unreachable job and released its
  lease.
- `not cancellable`: the job already reached a terminal state.

The server publishes the resulting state. A finalized cancellation emits the
same normal job update as any other `killed` result. It does not emit a watchdog
or worker-loss incident.

## Safety properties

- Worker silence never makes a device schedulable.
- In retention mode, worker silence never creates a terminal job result.
- Without retention mode, the current worker-loss behavior does not change.
- A returning process cannot make a device with another quarantine cause busy.
- Manual cancellation does not make an uncertain device schedulable.
- A queued cancellation never starts the job.
- Job history remains available after cancellation.
- Hold expiry remains bounded by its configured TTL.

## Tests

Store tests cover these cases:

- The default sweep still marks a silent worker's job `lost`.
- A retention-mode sweep makes the device unhealthy but leaves the job and
  lease live.
- In retention mode, job lease deadlines do not expire job leases.
- Hold deadlines still expire hold leases.
- A heartbeat that names the job restores a `worker_lost` device to `busy`.
- A heartbeat does not restore a device quarantined for another reason.
- Registration from a replacement worker still marks an old job `lost`.
- Killing a disconnected job finalizes it, releases its lease, and keeps its
  device unavailable.
- A finalized kill remains deliverable if the original worker returns.

Server and CLI tests cover queued cancellation through `rc kill`. They also
cover the flag default, the enabled flag, and a finalized disconnected job.
Existing connected-worker kill tests continue to cover asynchronous delivery.

## Documentation

Update the `rc serve` options, worker-loss section, and lease section in
`README.md`. State that the default behavior marks jobs `lost`. Document that
the flag keeps jobs active until reconnection, replacement-worker registration,
a terminal report, or `rc kill`.
