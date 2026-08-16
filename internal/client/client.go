// Package client is the typed HTTP client shared by every CLI verb.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
)

// ErrNoDevice mirrors the controller's 409: nothing free right now. A plain
// Submit queues instead of returning this; it only surfaces when the caller
// passes NoWait, opting out of the queue in favor of an immediate answer.
var ErrNoDevice = errors.New("no device available")

// ErrNotCancellable mirrors the controller's 409 not_cancellable from
// POST /v1/jobs/{id}/kill: the job raced onto a different state (started
// running, or already finished) between the caller's last look and this
// kill attempt. Exposed as a sentinel, rather than left for callers to
// string-match the message, because rc run needs to tell this apart from
// an ordinary kill failure: it means the job may now actually be running
// unattended and deserves Stage 1's honest "STILL RUNNING" warning.
var ErrNotCancellable = errors.New("job not cancellable")

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		// No global timeout: log streaming runs as long as the job does.
		http: &http.Client{},
	}
}

type SubmitOptions struct {
	DeviceID string
	// Selector picks a device by its labels instead of by exact ID. Give
	// exactly one of DeviceID or Selector.
	Selector       string
	Command        []string
	Cwd            string
	Env            map[string]string
	Submitter      string
	IdempotencyKey string
	// Priority orders queued jobs on the same device: higher runs sooner.
	Priority int
	// MaxRuntime and IdleTimeout are watchdog ceilings enforced by the
	// worker (task 5); zero means "use the device's configured default".
	MaxRuntime  time.Duration
	IdleTimeout time.Duration
	// NoWait opts out of stage 2's queue: a busy device fails fast with
	// ErrNoDevice instead of the job sitting queued behind it.
	NoWait bool
	// Kind is model.LeaseKindJob or model.LeaseKindHold; empty means job.
	// Callers submitting an ordinary job never set this — use Hold instead
	// of setting it here directly, since the controller rejects a hold
	// submission (Kind == model.LeaseKindHold) that also carries a Command.
	Kind string
	// Reason is why a hold was taken; meaningless for an ordinary job.
	Reason string
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}

func (c *Client) Submit(ctx context.Context, opts SubmitOptions) (*model.Job, error) {
	// The wire format (SubmitRequest.MaxRuntimeSeconds) is whole seconds; a
	// positive but sub-second value would truncate to 0 and be silently
	// treated as "no ceiling" at the controller instead of the tiny limit
	// actually requested. Reject it here, at the one point that can still
	// name what the caller asked for — matching the same rule the worker's
	// own device config enforces (internal/worker/config.go) for exactly
	// the same reason.
	if opts.MaxRuntime > 0 && opts.MaxRuntime < time.Second {
		return nil, fmt.Errorf(
			"max_runtime %s is below the one-second granularity runtime ceilings are enforced at", opts.MaxRuntime)
	}
	if opts.IdleTimeout > 0 && opts.IdleTimeout < time.Second {
		return nil, fmt.Errorf(
			"idle_timeout %s is below the one-second granularity runtime ceilings are enforced at", opts.IdleTimeout)
	}
	payload, err := json.Marshal(server.SubmitRequest{
		DeviceID: opts.DeviceID, Selector: opts.Selector, Command: opts.Command, Cwd: opts.Cwd, Env: opts.Env,
		Submitter: opts.Submitter, IdempotencyKey: opts.IdempotencyKey,
		Priority:           opts.Priority,
		MaxRuntimeSeconds:  int(opts.MaxRuntime.Seconds()),
		IdleTimeoutSeconds: int(opts.IdleTimeout.Seconds()),
		NoWait:             opts.NoWait,
		Kind:               opts.Kind,
		Reason:             opts.Reason,
	})
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodPost, "/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return nil, ErrNoDevice
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, apiError(resp)
	}
	var job model.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// HoldOptions configures rc hold: taking a device for a human to use
// directly (a shell), not for a job. Give exactly one of DeviceID or
// Selector, matching SubmitOptions.
type HoldOptions struct {
	DeviceID  string
	Selector  string
	Submitter string
	// Reason is why the device is being held (e.g. "manual profiling"),
	// shown by rc devices and the dashboard.
	Reason string
	// TTL is required: unlike an ordinary job's MaxRuntime, a hold has no
	// "device default" to fall back on, since the whole point is a human
	// deciding how long they need the device. Capped by the device's
	// max_runtime exactly as a job's MaxRuntime is — rejected, never
	// clamped.
	TTL time.Duration
}

// Hold submits a hold: a job with kind "hold" and no command of its own.
// The worker chooses the sleeper it actually runs (internal/worker's
// execute) — never this client, never the caller — so a hold can never be
// used to run arbitrary code under a different label; see
// server.handleSubmit, which rejects a hold submission that carries a
// command. Hold goes through the exact same Submit/Enqueue/ScheduleOnce
// path an ordinary job does: the same allocation transaction, the same
// queue, the same wall-clock watchdog for expiry.
func (c *Client) Hold(ctx context.Context, opts HoldOptions) (*model.Job, error) {
	return c.Submit(ctx, SubmitOptions{
		DeviceID:   opts.DeviceID,
		Selector:   opts.Selector,
		Submitter:  opts.Submitter,
		MaxRuntime: opts.TTL,
		Kind:       model.LeaseKindHold,
		Reason:     opts.Reason,
	})
}

// Release ends a hold — or, for that matter, any job — early. It is a thin
// alias over Kill so ownership is checked identically: only the job's own
// submitter, or an admin token, may release what they didn't hold.
func (c *Client) Release(ctx context.Context, jobID, submitter string) error {
	return c.Kill(ctx, jobID, submitter)
}

func (c *Client) Job(ctx context.Context, id string) (*model.Job, error) {
	view, err := c.JobView(ctx, id)
	if err != nil {
		return nil, err
	}
	return &view.Job, nil
}

// JobView fetches a job together with its queue position.
func (c *Client) JobView(ctx context.Context, id string) (*server.JobView, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+id, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp)
	}
	var view server.JobView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		return nil, err
	}
	return &view, nil
}

const (
	// scheduledPollInterval, scheduledPollTimeout and
	// maxScheduledPollFailures are WaitScheduled's own retry budget —
	// deliberately separate from WaitTerminal's pollTimeout/pollInterval/
	// maxPollFailures below, and tuned differently. WaitTerminal's 10s
	// per-poll bound would take well over a minute to give up on a
	// genuinely hung controller here (each of maxPollFailures+1 attempts
	// blocking for the full 10s) — too slow to be a reasonable
	// human-facing wait, and (before this was split out) the reason a
	// test covering that exact scenario was impractical to write.
	//
	// Instant-failing polls (connection refused, a 500 during a restart)
	// are tolerated for roughly maxScheduledPollFailures *
	// scheduledPollInterval = 16 * 500ms = 8s of wall clock — comfortably
	// more than the couple of seconds an ordinary controller restart
	// takes.
	//
	// A hung connection (accepted, never answered — exactly a wedged
	// controller) is bounded per attempt by scheduledPollTimeout and
	// detected within roughly (maxScheduledPollFailures+1) *
	// scheduledPollTimeout + maxScheduledPollFailures *
	// scheduledPollInterval ≈ 17s + 8s = 25s worst case — bounded well
	// under a minute, in production as well as in the test that exercises
	// it.
	scheduledPollInterval    = 500 * time.Millisecond
	scheduledPollTimeout     = 1 * time.Second
	maxScheduledPollFailures = 16
)

// WaitScheduled polls until the job leaves the queue, calling onPosition each
// time its position changes so the caller can show progress.
//
// "Leaves the queue" is exactly what it returns on, which is NOT the same as
// "was scheduled": a queued job can also be killed or cancelled, and that
// too ends the wait and is returned with no error. Callers must look at the
// state they get back before announcing a job as running — see
// cli.NewRunCmd, which reports a job that left the queue without ever
// starting as what it is rather than printing "job X on gpu0" and then
// streaming a log that will never have anything in it.
//
// Like WaitTerminal, it tolerates a bounded run of consecutive poll
// failures — a dropped connection, a controller restarting for a couple of
// seconds — with a fixed retry interval, resetting the count on every
// success, rather than giving up on the very first one (see the constants
// above for the resulting wall-clock budgets). This distinction matters
// more here than in WaitTerminal: a caller (rc run) that sees
// WaitScheduled give up may go on to cancel the job, since nothing is
// running yet and there is no lease to protect — but only if the job is
// actually still queued. A transient network blip must never be
// indistinguishable from "genuinely done waiting", or a brief controller
// hiccup can end up cancelling — or, worse, if the job was scheduled
// during the blip, flagging for kill — a job that was never in trouble.
//
// The giveup error, when the retry budget above is exhausted, is NOT
// distinguished from an ordinary error by its type or content — deciding
// "the caller's own bound elapsed" by unwrapping context.DeadlineExceeded
// from this error is unsound: each poll already runs under its own
// scheduledPollTimeout-bounded context, so a hung controller produces
// exactly that same error, with no caller deadline in sight at all.
// Callers must instead check their own context's Err() directly (see
// cli.NewRunCmd's waitCtx) to tell a real, caller-requested deadline
// apart from this function simply giving up.
func (c *Client) WaitScheduled(ctx context.Context, id string, onPosition func(int)) (*model.Job, error) {
	last := -1
	failures := 0
	for {
		pollCtx, cancel := context.WithTimeout(ctx, scheduledPollTimeout)
		view, err := c.JobView(pollCtx, id)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				// The caller's own context ended (Ctrl-C, or its own
				// --timeout deadline) — that is the caller's business, not
				// a giveup we need to explain.
				return nil, ctx.Err()
			}
			failures++
			if failures > maxScheduledPollFailures {
				return nil, fmt.Errorf(
					"giving up waiting for job %s to be scheduled after %d consecutive poll failures: %w",
					id, failures, err)
			}
		} else {
			failures = 0
			if view.Job.State != model.JobQueued {
				return &view.Job, nil
			}
			if view.QueuePosition != last {
				last = view.QueuePosition
				if onPosition != nil {
					onPosition(view.QueuePosition)
				}
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(scheduledPollInterval):
		}
	}
}

// Kill cancels a queued job or terminates a running one.
func (c *Client) Kill(ctx context.Context, id, submitter string) error {
	payload, err := json.Marshal(map[string]string{"submitter": submitter})
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodPost, "/v1/jobs/"+id+"/kill", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode == http.StatusConflict {
		var body struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if body.Message == "" {
			body.Message = "job not cancellable"
		}
		return fmt.Errorf("%w: %s", ErrNotCancellable, body.Message)
	}
	return apiError(resp)
}

// Retire removes a device from the fleet for good. Admin-only. The note the
// controller returns is passed back so the caller can repeat it: a worker
// that still declares this device will recreate it on its next registration.
func (c *Client) Retire(ctx context.Context, deviceID string) (string, error) {
	resp, err := c.do(ctx, http.MethodDelete, "/v1/devices/"+url.PathEscape(deviceID), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", apiError(resp)
	}
	var body struct {
		Note string `json:"note"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return body.Note, nil
}

// StreamLogs copies the job's output to out until the job finishes.
func (c *Client) StreamLogs(ctx context.Context, id string, out io.Writer) error {
	resp, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+id+"/logs", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return apiError(resp)
	}
	_, err = io.Copy(out, resp.Body)
	return err
}

// Describe fetches everything `rc describe` shows about one device: its
// state and holder, every label with its provenance and age, the usage
// sheet and when it was last written, and recent job history.
func (c *Client) Describe(ctx context.Context, deviceID string) (*server.DescribeResponse, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/devices/"+deviceID+"/describe", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp)
	}
	var out server.DescribeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Explain answers "if I submitted this selector right now, what would
// happen" without submitting anything: which devices match, which of those
// are free, and how deep the queue already is behind the ones that aren't.
func (c *Client) Explain(ctx context.Context, selector string) (*server.ExplainResponse, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/explain?selector="+url.QueryEscape(selector), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp)
	}
	var out server.ExplainResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) State(ctx context.Context) (*server.StateResponse, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/state", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp)
	}
	var state server.StateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

const (
	pollInterval = 500 * time.Millisecond
	// pollTimeout bounds each individual state request. Without it a wedged
	// controller connection (TCP established, nothing ever comes back) hangs
	// the whole call, since the client's http.Client deliberately has no
	// global timeout.
	pollTimeout = 10 * time.Second
	// maxPollFailures tolerates a short run of transient errors (a dropped
	// connection, a 500) without giving up on a job that may still be
	// running perfectly fine. It resets on every successful poll.
	maxPollFailures = 5
)

// WaitTerminal polls until the job reaches a terminal state. maxWait bounds
// the whole call so a job stuck in "assigned" (no worker ever attached) or a
// controller that stops responding cannot hang it forever; pass 0 for no
// bound. On giveup — either the overall bound or too many consecutive poll
// failures — the returned error names the job and its last known state
// rather than leaving the caller with a bare timeout.
func (c *Client) WaitTerminal(ctx context.Context, id string, maxWait time.Duration) (*model.Job, error) {
	overall := ctx
	if maxWait > 0 {
		var cancel context.CancelFunc
		overall, cancel = context.WithTimeout(ctx, maxWait)
		defer cancel()
	}

	var lastJob *model.Job
	var lastErr error
	failures := 0

	for {
		pollCtx, cancel := context.WithTimeout(overall, pollTimeout)
		job, err := c.Job(pollCtx, id)
		cancel()

		if err == nil {
			failures = 0
			lastJob = job
			if job.State.Terminal() {
				return job, nil
			}
		} else {
			if ctx.Err() != nil {
				// The caller's own context ended (e.g. rc run detaching on
				// Ctrl-C) — that is the caller's business, not a giveup we
				// need to explain.
				return nil, ctx.Err()
			}
			failures++
			lastErr = err
			if failures > maxPollFailures {
				return nil, fmt.Errorf("giving up on job %s after %d consecutive poll failures (last known state: %s): %w",
					id, failures, lastJobState(lastJob), lastErr)
			}
		}

		select {
		case <-overall.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("timed out after %s waiting for job %s to finish (last known state: %s)",
				maxWait, id, lastJobState(lastJob))
		case <-time.After(pollInterval):
		}
	}
}

func lastJobState(job *model.Job) string {
	if job == nil {
		return "unknown"
	}
	return string(job.State)
}

func apiError(resp *http.Response) error {
	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "" {
		return fmt.Errorf("%s: %s", body.Error, body.Message)
	}
	return fmt.Errorf("controller returned %s", resp.Status)
}
