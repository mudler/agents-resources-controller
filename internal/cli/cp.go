package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/mudler/resource-controller/internal/client"
)

// parseCpArg splits one `rc cp` argument into "is this on a box, and where".
//
// A remote path is <host>:<name>:<path> — dgx:gpu0:/workspace/x — and the
// awkward part is that a device ID contains a colon of its own, which is
// exactly the ambiguity naive splitting gets wrong. So: split on the LAST
// colon, and require what is left to be a device ID (one colon, both halves
// non-empty). Anything else is a local path, colons and all.
//
// The failure this shape prevents is asymmetric, which is why it refuses
// rather than guesses. Mistaking a local path for a remote one sends a file
// to a device that does not exist and fails loudly; mistaking a remote path
// for a local one writes to the local filesystem and reports success.
func parseCpArg(arg string) (remote bool, device, path string, err error) {
	i := strings.LastIndex(arg, ":")
	if i < 0 {
		return false, "", arg, nil
	}

	head, tail := arg[:i], arg[i+1:]
	host, name, ok := strings.Cut(head, ":")
	if !ok || host == "" || name == "" || strings.Contains(name, ":") {
		// One colon is not a device: "./a:b" and "C:\tmp" are local paths.
		// But something that LOOKS like it was meant to be remote — a single
		// colon with a path after it — is far more often a typo than a file
		// literally named that, so say what the shape is.
		if !ok && tail != "" && strings.HasPrefix(tail, "/") {
			return false, "", "", fmt.Errorf(
				"%q is not a device path: a remote path is host:name:path (e.g. dgx:gpu0%s)", arg, tail)
		}
		return false, "", arg, nil
	}
	if tail == "" {
		return false, "", "", fmt.Errorf(
			"%q names a device but no path: a remote path is host:name:path (e.g. %s:/workspace/)", arg, head)
	}
	return true, head, tail, nil
}

// NewCpCmd is `rc cp`: move a file you name, onto a box you hold, once.
//
// It is not a deployment tool. It does not sync, watch, reconcile or install,
// and anything recurring or large belongs on the NAS mount the worker already
// sees — every byte of a copy crosses the controller twice, and the
// controller is a scheduler with a one-connection database, not a file
// server.
func NewCpCmd() *cobra.Command {
	var as string

	cmd := &cobra.Command{
		Use:   "cp <src> <dst>",
		Short: "Copy a file or directory to or from a device you hold",
		Long: "Copy a file or directory between here and a device.\n\n" +
			"One side must be remote, written host:name:path — e.g.\n" +
			"  rc cp ./train.py dgx:gpu0:/workspace/\n" +
			"  rc cp dgx:gpu0:/workspace/result.json ./\n\n" +
			"The copy runs as a job under a lease, so you can only copy to a device\n" +
			"that is free for you to take. Models, datasets and checkpoints belong on\n" +
			"the shared NAS mount, not here.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			srcRemote, srcDevice, srcPath, err := parseCpArg(args[0])
			if err != nil {
				return err
			}
			dstRemote, dstDevice, dstPath, err := parseCpArg(args[1])
			if err != nil {
				return err
			}

			switch {
			case srcRemote && dstRemote:
				return fmt.Errorf("both %q and %q are on a device: rc cp moves a file between here and one box, not between two boxes", args[0], args[1])
			case !srcRemote && !dstRemote:
				return fmt.Errorf("neither %q nor %q names a device: one side must be host:name:path", args[0], args[1])
			}

			submitter := as
			if submitter == "" {
				submitter = defaultSubmitter()
			}
			c := client.New(controllerURL(), controllerToken())

			if dstRemote {
				return c.CopyTo(cmd.Context(), client.CopyOptions{
					DeviceID: dstDevice, Local: srcPath, Remote: dstPath,
					Submitter: submitter, Progress: cmd.ErrOrStderr(),
				})
			}
			return c.CopyFrom(cmd.Context(), client.CopyOptions{
				DeviceID: srcDevice, Local: dstPath, Remote: srcPath,
				Submitter: submitter, Progress: cmd.ErrOrStderr(),
			})
		},
	}

	cmd.Flags().StringVar(&as, "as", "", "identity shown in rc ps (defaults to $RC_SUBMITTER, else user@host/session)")
	return cmd
}
