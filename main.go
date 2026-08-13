package main

import (
	"errors"
	"fmt"
	"os"

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
	root.AddCommand(cli.NewRunCmd())

	if err := root.Execute(); err != nil {
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			os.Exit(coded.ExitCode())
		}
		fmt.Fprintln(os.Stderr, "rc:", err)
		os.Exit(1)
	}
}
