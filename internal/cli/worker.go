package cli

import (
	"context"
	"errors"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/spf13/cobra"
)

func NewWorkerCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Run the device-host agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := worker.LoadConfig(configPath)
			if err != nil {
				return err
			}
			// Start returns ctx.Err() once shutdown completes, even on an
			// ordinary SIGTERM — that is the right contract for the worker
			// package (its callers may care whether a shutdown was clean vs.
			// forced), but surfacing context.Canceled as this command's error
			// would make main.go report exit 1 and print "context canceled"
			// for a worker that drained its jobs and stopped exactly as
			// asked. Under systemd's default restart-on-failure that turns
			// every graceful `systemctl stop` into a restart loop, so a
			// signal-driven shutdown must exit 0 here.
			if err := worker.New(cfg).Start(cmd.Context()); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "/etc/rc/worker.yaml", "worker config file")
	return cmd
}
