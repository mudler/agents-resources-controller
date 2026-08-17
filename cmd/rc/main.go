package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "rc",
		Short:         "Resource controller: exclusive device leases for shared hardware",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(cli.NewRunCmd(), cli.NewPsCmd(), cli.NewDevicesCmd(),
		cli.NewServeCmd(), cli.NewWorkerCmd(), cli.NewKillCmd(), cli.NewAttachCmd(),
		cli.NewDescribeCmd(), cli.NewHoldCmd(), cli.NewReleaseCmd(),
		cli.NewRetireCmd(), cli.NewClearCmd())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// stop() must run the instant the first signal lands, not only when
	// main() returns: signal.NotifyContext keeps intercepting SIGINT/SIGTERM
	// (suppressing the OS's normal terminate-on-signal action) until stop is
	// called, and a bare `defer stop()` doesn't fire until root.ExecuteContext
	// itself returns — which, for a long shutdown (e.g. rc worker's 45s grace
	// while it drains an in-flight job), can be tens of seconds later. Without
	// this, a second Ctrl-C during that window is silently absorbed instead of
	// force-killing the process, and it also defeats run.go's own
	// detachIfInterrupted, which only ever unregisters its own inner
	// signal.NotifyContext layer, not this outer one.
	go func() {
		<-ctx.Done()
		stop()
	}()

	if err := root.ExecuteContext(ctx); err != nil {
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			os.Exit(coded.ExitCode())
		}
		fmt.Fprintln(os.Stderr, "rc:", err)
		os.Exit(1)
	}
}
