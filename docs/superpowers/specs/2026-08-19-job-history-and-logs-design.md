# Job history and stored logs design

## Goal

Add CLI commands that find past jobs and read their stored output. Keep the
existing live log behavior compatible with `rc attach` and `rc run`.

The feature adds these commands:

```sh
rc jobs
rc jobs --device gpubox:gpu0 --state failed
rc logs <job-id>
rc logs -f <job-id>
```

## Scope

`rc jobs` lists fleet-wide job history. It includes queued, active, and terminal
jobs. The command supports exact filters for device, submitter, and state.

`rc logs` reads the append-only output that the controller stores for a job.
The command prints the current snapshot by default. The `-f` or `--follow`
flag keeps the stream open until the job reaches a terminal state.

The change does not add log retention, search inside logs, time-range filters,
or cursor pagination. It does not store output from TTY or pipe jobs.

## Job history API

Add an authenticated client route:

```text
GET /v1/jobs
```

The route accepts these query parameters:

- `limit`: Number of jobs to return. The default is `20`. The valid range is
  `1..200`.
- `device`: Exact device ID.
- `submitter`: Exact submitter value.
- `state`: One valid `model.JobState` value.

The server combines supplied filters with AND. The server rejects an invalid
limit or state with HTTP 400. Empty filter values act as absent filters.

The response has an envelope so later versions can add paging data without
changing the job array:

```json
{
  "jobs": []
}
```

The controller orders jobs by `submitted_at DESC, id DESC`. The ID tie-breaker
makes results deterministic when jobs have equal submission timestamps.

The route requires the existing `client` role. It uses the same fleet-wide
visibility as `rc ps`, `rc devices`, and the dashboard.

## Store query

Add a store filter type and a `ListJobs` method. The filter contains the limit,
device ID, submitter, and state.

The method builds one parameterized SQL query. It does not fetch each job in a
separate query. The method reuses the existing job row scanner so job decoding
stays consistent with `Store.Job` and recent device history.

The server validates public input before it calls the store. The store still
enforces a positive bounded limit for callers inside the process.

No schema migration is required. The existing `jobs` table contains all fields
that the API returns.

## Job history client and CLI

Add a typed client request type for the list filters. Add a client method that
encodes supplied filters with `url.Values` and decodes the response envelope.

Add `rc jobs` with these flags:

```text
-n, --limit int          maximum jobs to return (default 20)
    --device string      filter by exact device ID
    --submitter string   filter by exact submitter
    --state string       filter by exact job state
-o, --output string      output format: text or json (default text)
```

Text output uses this table:

```text
JOB  DEVICE  STATE  SUBMITTER  STARTED  DURATION  COMMAND
```

The table uses `-` when a job has no device, start time, or duration. It joins
the command with spaces, consistent with `rc ps`. It includes the kill reason
beside the state when the reason is not empty. It prints start times in UTC with
RFC 3339 format. It rounds durations to the nearest second. For an active job,
the CLI measures duration from its start time to the current time.

The JSON format writes the complete array of stored job objects. It does not
write diagnostic text to stdout. The command rejects output formats other than
`text` and `json` before it sends a request.

## Stored log API

Keep this route and add an optional query parameter:

```text
GET /v1/jobs/{id}/logs?follow=false
```

An absent `follow` value preserves the current follow behavior. This rule keeps
existing clients compatible. The value `true` also selects follow behavior.
The value `false` selects snapshot behavior. The server rejects other values
with HTTP 400.

The server looks up the job before it opens or reads a log file. An unknown job
returns HTTP 404 and does not create a file.

For snapshot behavior, the server reads the stored file once and writes its
bytes with the existing plain-text content type. A missing file for a normal
log job is an empty snapshot. The request does not wait for job completion.

For follow behavior, the server uses the existing log-store follower. It
replays bytes from the start and waits for more bytes until the job terminates.
Existing write deadlines and disconnect cleanup remain in effect.

Jobs with `stdio` set to `tty` or `pipe` do not have stored output. A snapshot
request returns HTTP 409 with the code `logs_not_stored` for those jobs. The
message states which stdio mode prevented storage. A follow request preserves
the existing stream behavior for compatibility with `rc attach`.

## Stored log client and CLI

Add a typed client method that accepts a follow boolean. The method fetches the
job first and returns `logs_not_stored` for a TTY or pipe job. Keep `StreamLogs`
as a compatibility wrapper that directly requests follow behavior. Existing
`rc run` and `rc attach` behavior does not change.

Add this command:

```text
rc logs [-f|--follow] <job-id>
```

Without `--follow`, the command requests `follow=false`. It writes the snapshot
to `cmd.OutOrStdout()` and exits after the response body ends.

With `--follow`, the command requests `follow=true`. It replays stored bytes and
waits for new bytes until the job terminates. A user interrupt exits with code
130 and does not print a context cancellation error.

The command writes no headers or status messages to stdout. This behavior keeps
commands such as `rc logs <id> > run.log` and `rc logs <id> | grep error`
safe.

`rc attach` remains available. Its help continues to describe its always-follow
behavior. The new `rc logs` command is the primary documented command for
stored output.

## Errors

The API uses the existing JSON error shape. The CLI returns those errors through
the root command's existing stderr path.

The feature handles these cases explicitly:

- An unknown job returns `not_found`.
- A TTY or pipe job returns `logs_not_stored`.
- An unknown state returns `bad_request`.
- A malformed or out-of-range limit returns `bad_request`.
- An unknown output format fails locally.
- A failed stdout write returns an error instead of reporting success.
- An interrupted follow exits with code 130.

An empty snapshot is successful for a normal job that produced no output.

## Tests

Add store tests for newest-first ordering, the ID tie-breaker, the default
limit, explicit limits, each exact filter, combined filters, and empty results.

Add server tests for authentication, response shape, filter forwarding, limit
and state validation, snapshot behavior, follow behavior, unknown jobs, empty
normal logs, and `logs_not_stored` responses.

Add client tests for query encoding, response decoding, snapshot selection,
follow selection, and API error propagation.

Add CLI tests for table rendering, JSON output, combined filters, invalid output
formats, pipe-safe log bytes, write failures, and interrupt exit code 130.

Add an end-to-end test that runs a job, finds it with `rc jobs`, and reads its
completed output with `rc logs`.

## Documentation

Update the README command reference. Explain that `rc jobs` finds old IDs and
that `rc logs` reads stored output after completion. Document `--follow` and the
absence of stored output for TTY and pipe jobs.

Update `docs/agents.md` with the same discovery and retrieval workflow. Keep
`rc attach` documented as an always-follow compatibility command.
