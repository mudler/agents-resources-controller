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
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
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
	DeviceID       string
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
	payload, err := json.Marshal(server.SubmitRequest{
		DeviceID: opts.DeviceID, Command: opts.Command, Cwd: opts.Cwd, Env: opts.Env,
		Submitter: opts.Submitter, IdempotencyKey: opts.IdempotencyKey,
		Priority:           opts.Priority,
		MaxRuntimeSeconds:  int(opts.MaxRuntime.Seconds()),
		IdleTimeoutSeconds: int(opts.IdleTimeout.Seconds()),
		NoWait:             opts.NoWait,
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

// WaitScheduled polls until the job leaves the queue, calling onPosition each
// time its position changes so the caller can show progress.
//
// Like WaitTerminal, it tolerates a bounded run of consecutive poll
// failures — a dropped connection, a controller restarting for a couple of
// seconds — with a fixed retry interval, resetting the count on every
// success, rather than giving up on the very first one. This distinction
// matters more here than in WaitTerminal: a caller (rc run) that sees
// WaitScheduled give up may go on to cancel the job, since nothing is
// running yet and there is no lease to protect — but only if the job is
// actually still queued. A transient network blip must never be
// indistinguishable from "genuinely done waiting", or a brief controller
// hiccup can end up cancelling — or, worse, if the job was scheduled
// during the blip, flagging for kill — a job that was never in trouble.
// The giveup error is deliberately not context.DeadlineExceeded: callers
// use that specific sentinel to recognize a real, caller-requested
// deadline (e.g. --timeout) as opposed to an error this function does not
// understand.
func (c *Client) WaitScheduled(ctx context.Context, id string, onPosition func(int)) (*model.Job, error) {
	last := -1
	failures := 0
	for {
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
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
			if failures > maxPollFailures {
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
		case <-time.After(pollInterval):
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
