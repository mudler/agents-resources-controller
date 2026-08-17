package client

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/model"
)

// `rc cp` is tar over the relay, which is how kubectl cp is built and for the
// same reason: once the exec stream carries bytes both ways, copying a file
// needs no new endpoint, no upload storage and no controller-side buffering.
// The archive is streamed, never assembled — a 2GB checkpoint would otherwise
// be held twice — and the controller keeps none of it.

// copyWait bounds how long a copy waits for its job to finish once the
// archive has been handed over. Generous, because the far side may be writing
// a large file to slow storage, but bounded: a client that hangs forever on a
// worker that vanished is worse than one that says so.
const copyWait = 30 * time.Minute

// CopyOptions is one transfer.
type CopyOptions struct {
	// DeviceID is the box, always — a copy has exactly one remote side.
	DeviceID string
	// Local is the path on this machine, Remote the path on the box.
	Local  string
	Remote string
	// Submitter is who the copy runs as, and therefore who holds the lease
	// it takes.
	Submitter string
	// Progress, if non-nil, receives one-line status notes.
	Progress io.Writer
}

func (o CopyOptions) note(format string, args ...any) {
	if o.Progress != nil {
		fmt.Fprintf(o.Progress, "rc: "+format+"\n", args...)
	}
}

// CopyTo streams local onto the device, extracting it at remote.
//
// The far side runs `tar -xf -`, reading the archive from the job's stdin,
// which is the relay's input direction. Nothing here builds the archive first:
// the tar writer writes straight into the socket as the walk produces
// entries.
func (c *Client) CopyTo(ctx context.Context, o CopyOptions) error {
	info, err := os.Lstat(o.Local)
	if err != nil {
		return fmt.Errorf("read %s: %w", o.Local, err)
	}

	// Trailing slash means "into this directory, keeping the source's name",
	// which is scp's rule and the one people already have in their fingers.
	dir, name := path.Split(strings.TrimSuffix(o.Remote, "/"))
	if strings.HasSuffix(o.Remote, "/") || name == "" {
		dir, name = strings.TrimSuffix(o.Remote, "/"), filepath.Base(o.Local)
	}
	if dir == "" {
		dir = "."
	}

	// $0, not string interpolation: a directory with a space, a quote or a $
	// in it would otherwise be a shell injection into a command running on a
	// GPU box as root.
	job, err := c.submitCopyJob(ctx, o, []string{
		"/bin/sh", "-c", `mkdir -p "$0" && exec tar -xf - -C "$0"`, strings.TrimSuffix(dir, "/"),
	})
	if err != nil {
		return err
	}
	o.note("copying %s to %s:%s (job %s)", o.Local, o.DeviceID, o.Remote, job.ID)

	streams, err := c.attachRelay(ctx, job.ID)
	if err != nil {
		return err
	}
	defer streams.close()

	// The far side's stdout, which for `tar -x` is silence. Drained anyway:
	// a stream nobody reads eventually blocks the worker's write, and a
	// blocked worker is a stuck copy.
	go func() { _, _ = io.Copy(io.Discard, streams.out) }()

	writeErr := writeTarStream(streams.in, o.Local, name, info)
	// Closing input is the EOF that tells tar the archive is complete. It
	// must happen even when the walk failed, or the far side waits forever
	// for the rest of an archive that is never coming.
	closeErr := streams.in.Close()
	if writeErr != nil {
		return fmt.Errorf("send %s: %w", o.Local, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("finish sending %s: %w", o.Local, closeErr)
	}

	return c.awaitCopy(ctx, job.ID)
}

// CopyFrom streams remote off the device into local.
//
// The mirror image: `tar -cf -` on the box writes the archive to its stdout,
// which is the relay's output direction, and it is extracted here. The remote
// end is not trusted with where those bytes land — see extractTar.
func (c *Client) CopyFrom(ctx context.Context, o CopyOptions) error {
	remote := strings.TrimSuffix(o.Remote, "/")
	dir, name := path.Split(remote)
	if dir == "" {
		dir = "."
	}
	if name == "" {
		return fmt.Errorf("%q names no file to copy", o.Remote)
	}

	// Where it lands here: into a directory if the local path is one (or is
	// written as one), otherwise the single top-level entry takes the local
	// path's own name.
	destDir, rename := o.Local, ""
	if fi, err := os.Stat(o.Local); (err == nil && fi.IsDir()) || strings.HasSuffix(o.Local, "/") {
		destDir = strings.TrimSuffix(o.Local, "/")
	} else {
		destDir, rename = filepath.Dir(o.Local), filepath.Base(o.Local)
	}
	if destDir == "" {
		destDir = "."
	}

	job, err := c.submitCopyJob(ctx, o, []string{
		"/bin/sh", "-c", `exec tar -cf - -C "$0" -- "$1"`, strings.TrimSuffix(dir, "/"), name,
	})
	if err != nil {
		return err
	}
	o.note("fetching %s:%s to %s (job %s)", o.DeviceID, o.Remote, o.Local, job.ID)

	streams, err := c.attachRelay(ctx, job.ID)
	if err != nil {
		return err
	}
	defer streams.close()

	// `tar -c` never reads its stdin. Close it at once so the far side is
	// not held open by a direction it has no use for.
	_ = streams.in.Close()

	if err := extractTar(streams.out, destDir, rename); err != nil {
		return fmt.Errorf("receive %s:%s: %w", o.DeviceID, o.Remote, err)
	}
	return c.awaitCopy(ctx, job.ID)
}

// ErrDeviceHeldByAnother means the device is leased to somebody else. A copy
// runs under a lease exactly like any other execution, so this is the whole
// of "there is no unauthenticated upload path".
var ErrDeviceHeldByAnother = errors.New("device is held by someone else")

// submitCopyJob takes the lease and starts the far-side tar.
//
// The holder check happens BEFORE submitting, so a copy aimed at a box
// somebody else is using is refused with their name on it rather than queued
// behind them — a transfer that lands in forty minutes when the current job
// finishes is not what anybody typing `rc cp` meant. NoWait is the backstop
// for the race between the check and the submit.
func (c *Client) submitCopyJob(ctx context.Context, o CopyOptions, command []string) (*model.Job, error) {
	state, err := c.State(ctx)
	if err != nil {
		return nil, err
	}
	found := false
	for _, d := range state.Devices {
		if d.Device.ID != o.DeviceID {
			continue
		}
		found = true
		if d.Holder != "" && d.Holder != o.Submitter {
			return nil, fmt.Errorf("%w: %s is held by %s%s — a copy runs under a lease, and it is not yours to take",
				ErrDeviceHeldByAnother, o.DeviceID, d.Holder, holdDetail(d.Kind))
		}
		if d.Holder == o.Submitter {
			return nil, fmt.Errorf(
				"%s is already held by you (job %s): a copy needs the device itself, so release that lease first, or copy from inside the session",
				o.DeviceID, d.JobID)
		}
	}
	if !found {
		return nil, fmt.Errorf("no such device: %s", o.DeviceID)
	}

	job, err := c.Submit(ctx, SubmitOptions{
		DeviceID:       o.DeviceID,
		Command:        command,
		Submitter:      o.Submitter,
		IdempotencyKey: uuid.NewString(),
		Stdio:          model.StdioPipe,
		NoWait:         true,
	})
	if errors.Is(err, ErrNoDevice) {
		return nil, fmt.Errorf("%w: %s was taken between checking it and asking for it", ErrDeviceHeldByAnother, o.DeviceID)
	}
	return job, err
}

func holdDetail(kind string) string {
	if kind == model.LeaseKindHold {
		return " (a hold)"
	}
	return ""
}

// awaitCopy waits for the far-side tar to finish and turns a non-zero exit
// into an error carrying what it complained about. tar's stderr is the job's
// log — stdout is the payload and cannot be shared with diagnostics — so that
// is where the reason comes from.
func (c *Client) awaitCopy(ctx context.Context, jobID string) error {
	final, err := c.WaitTerminal(ctx, jobID, copyWait)
	if err != nil {
		return err
	}
	if final.State == model.JobSucceeded {
		return nil
	}
	reason := final.KillReason
	var logs strings.Builder
	if err := c.StreamLogs(ctx, jobID, &logs); err == nil && logs.Len() > 0 {
		reason = strings.TrimSpace(logs.String())
	}
	if reason == "" {
		reason = "no further detail available"
	}
	return fmt.Errorf("copy job %s %s: %s", jobID, final.State, reason)
}

// writeTarStream walks src and writes it into w as a tar archive, entry by
// entry, with nothing buffered beyond one copy buffer.
//
// asName is what the top-level entry is called on the far side, which is how
// `rc cp ./a.py box:/tmp/b.py` renames on arrival.
func writeTarStream(w io.Writer, src, asName string, info os.FileInfo) error {
	tw := tar.NewWriter(w)

	if !info.IsDir() {
		if err := writeTarEntry(tw, src, asName, info); err != nil {
			return err
		}
		return tw.Close()
	}

	err := filepath.Walk(src, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		name := asName
		if rel != "." {
			name = path.Join(asName, filepath.ToSlash(rel))
		}
		return writeTarEntry(tw, p, name, fi)
	})
	if err != nil {
		return err
	}
	return tw.Close()
}

// writeTarEntry writes one entry.
//
// filepath.Walk uses Lstat, so a symlink arrives here AS a symlink and is
// archived as one — its target is never opened. That is the rule, not an
// implementation detail: following links would let a link inside the source
// tree pull in /etc/shadow, or ../../.ssh/id_ed25519, and ship it to a box.
//
// The mode comes off the FileInfo through tar.FileInfoHeader, which is the
// one header field that decides whether a copied script is executable when it
// lands. It is one line and it is the difference between this feature working
// and this feature feeling broken.
func writeTarEntry(tw *tar.Writer, p, name string, fi os.FileInfo) error {
	link := ""
	if fi.Mode()&os.ModeSymlink != 0 {
		var err error
		if link, err = os.Readlink(p); err != nil {
			return err
		}
	}

	hdr, err := tar.FileInfoHeader(fi, link)
	if err != nil {
		return err
	}
	hdr.Name = name
	if fi.IsDir() {
		hdr.Name = name + "/"
	}
	// Ownership is deliberately dropped: the numeric uid/gid here mean
	// nothing on the box, and a tar carrying them extracted as root would
	// create files owned by whoever happens to hold that id there.
	hdr.Uid, hdr.Gid = 0, 0
	hdr.Uname, hdr.Gname = "", ""

	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return nil
	}

	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(tw, f)
	return err
}

// extractTar unpacks r into destDir. If rename is non-empty, the first path
// component of every entry is replaced with it, which is how
// `rc cp box:/tmp/a.py ./b.py` lands under the name the caller asked for.
//
// The archive comes off a machine this client does not get to trust — a
// broken tar, a job someone else's command produced — so every entry's
// destination is checked to be inside destDir before anything is created. An
// archive containing ../../.ssh/authorized_keys is a way to take over the
// machine running the copy, and "we ran their tar and it was fine" is not a
// security model.
func extractTar(r io.Reader, destDir, rename string) error {
	root, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}

	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		name := hdr.Name
		if rename != "" {
			parts := strings.SplitN(strings.TrimPrefix(path.Clean(name), "./"), "/", 2)
			parts[0] = rename
			name = strings.Join(parts, "/")
		}

		target, err := safeJoin(root, name)
		if err != nil {
			return err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode).Perm()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			// The link's TARGET is checked too, not only the entry's own
			// path: a symlink placed inside the destination that points at
			// /etc is how the next entry in the same archive writes outside
			// it, and a link nobody looked at is exactly how that works.
			if _, err := safeJoin(filepath.Dir(target), hdr.Linkname); err != nil {
				return fmt.Errorf("archive contains a symlink out of the destination: %s -> %s", hdr.Name, hdr.Linkname)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(hdr.Mode).Perm())
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			// Explicitly, because OpenFile's mode is masked by umask and an
			// executable that arrives without its executable bit is the most
			// common way this feature disappoints.
			if err := os.Chmod(target, os.FileMode(hdr.Mode).Perm()); err != nil {
				return err
			}
		default:
			// Devices, fifos, hard links: not what this is for, and each one
			// is a way to create something surprising as whoever is running
			// the copy. Skipped rather than fatal, so one odd entry in a
			// directory does not lose the rest of it.
			continue
		}
	}
}

// safeJoin resolves name under root and refuses anything that escapes it.
// The check is on the CLEANED path, because "a/../../b" contains no leading
// ".." and escapes anyway.
func safeJoin(root, name string) (string, error) {
	if path.IsAbs(name) || filepath.IsAbs(name) {
		return "", fmt.Errorf("archive entry has an absolute path: %s", name)
	}
	target := filepath.Join(root, filepath.FromSlash(name))
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return "", fmt.Errorf("archive entry escapes the destination: %s", name)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive entry escapes the destination: %s", name)
	}
	return target, nil
}
