package cli

import (
	"github.com/mudler/resource-controller/internal/worker"
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
			return worker.New(cfg).Start(cmd.Context())
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "/etc/rc/worker.yaml", "worker config file")
	return cmd
}
