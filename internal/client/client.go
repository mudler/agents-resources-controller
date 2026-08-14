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
	payload, err := json.Marshal(server.SubmitRequest{
		DeviceID: opts.DeviceID, Command: opts.Command, Cwd: opts.Cwd, Env: opts.Env,
		Submitter: opts.Submitter, IdempotencyKey: opts.IdempotencyKey,
		NoWait: opts.NoWait,
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
	return &view.Job, nil
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
