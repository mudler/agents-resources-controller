package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "rc",
		Short:         "Resource controller: exclusive device leases for shared hardware",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "rc:", err)
		os.Exit(1)
	}
}
