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
		cli.NewServeCmd(), cli.NewWorkerCmd())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := root.ExecuteContext(ctx); err != nil {
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			os.Exit(coded.ExitCode())
		}
		fmt.Fprintln(os.Stderr, "rc:", err)
		os.Exit(1)
	}
}
