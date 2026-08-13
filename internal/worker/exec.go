// Package worker runs jobs on a device host and reports back to the controller.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type JobSpec struct {
	Command      []string
	Cwd          string
	Env          map[string]string
	GraceCeiling time.Duration // SIGTERM -> SIGKILL window; default 10s
}

type Result struct {
	ExitCode int
	Killed   bool
	Reason   string
	Err      error
}

// Run spawns the command in its OWN process group and merges stdout and stderr
// into sink. On cancellation the whole group is signalled, so children that
// outlive their parent — the ones still holding device memory — die too.
func Run(ctx context.Context, spec JobSpec, sink io.Writer) Result {
	if len(spec.Command) == 0 {
		return Result{ExitCode: -1, Err: errors.New("empty command")}
	}
	grace := spec.GraceCeiling
	if grace <= 0 {
		grace = 10 * time.Second
	}

	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = sink
	cmd.Stderr = sink
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return Result{ExitCode: -1, Err: fmt.Errorf("start: %w", err)}
	}
	pgid := cmd.Process.Pid // Setpgid makes the child its own group leader.

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	killGroup := func(sig syscall.Signal) {
		// Negative pid addresses the whole group.
		_ = syscall.Kill(-pgid, sig)
	}

	var killed bool
	var reason string

	select {
	case err := <-waitErr:
		return finish(err, killed, reason)
	case <-ctx.Done():
		killed = true
		reason = "cancelled"
		killGroup(syscall.SIGTERM)

		select {
		case err := <-waitErr:
			return finish(err, killed, reason)
		case <-time.After(grace):
			killGroup(syscall.SIGKILL)
			err := <-waitErr
			return finish(err, killed, reason)
		}
	}
}

func finish(err error, killed bool, reason string) Result {
	res := Result{Killed: killed, Reason: reason}
	if err == nil {
		return res
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		res.ExitCode = ee.ExitCode()
		if res.ExitCode < 0 {
			res.ExitCode = 137 // killed by signal
		}
		return res
	}
	res.ExitCode = -1
	res.Err = err
	return res
}
