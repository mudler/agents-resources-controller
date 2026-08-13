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

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
)

// ErrNoDevice mirrors the controller's 409: nothing free right now. Stage 1
// has no queue, so the caller decides whether to retry.
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
	var job model.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
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

// WaitTerminal polls until the job reaches a terminal state.
func (c *Client) WaitTerminal(ctx context.Context, id string) (*model.Job, error) {
	for {
		job, err := c.Job(ctx, id)
		if err != nil {
			return nil, err
		}
		if job.State.Terminal() {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
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
