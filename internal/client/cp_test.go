package client_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// farEnd is a controller and a worker in one, small enough to read: it hands
// out one device, and when a job is submitted it actually RUNS the command on
// this machine with its stdin and stdout spliced onto the relay's two halves.
//
// The far side is therefore a real `tar`, which is the whole point of testing
// it this way: the assertion is "the file arrived with the right bytes and
// the right mode", not "we wrote something to a socket". A hand-rolled fake
// tar would agree with whatever this package happened to emit.
type farEnd struct {
	ts *httptest.Server

	// holder is who the fleet says has the device. Empty means free.
	holder string

	mu        sync.Mutex
	job       model.Job
	submitted server.SubmitRequest
	stderr    bytes.Buffer

	release func()
}

func newFarEnd(t *testing.T) *farEnd {
	t.Helper()
	f := &farEnd{job: model.Job{ID: "j1", DeviceID: "dgx:gpu0"}}

	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	f.release = func() { outR.Close(); outW.Close(); inR.Close(); inW.Close() }

	f.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		switch {
		case r.URL.Path == "/v1/state":
			f.mu.Lock()
			holder := f.holder
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(server.StateResponse{Devices: []server.DeviceView{{
				Device: model.Device{ID: "dgx:gpu0", Host: "dgx", Name: "gpu0", State: model.DeviceReady},
				Holder: holder,
				JobID:  "someone-elses-job",
			}}})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			var req server.SubmitRequest
			_ = json.NewDecoder(r.Body).Decode(&req)

			f.mu.Lock()
			f.submitted = req
			f.job.State = model.JobRunning
			f.mu.Unlock()

			cmd := exec.Command(req.Command[0], req.Command[1:]...)
			cmd.Stdin = inR
			cmd.Stdout = outW
			cmd.Stderr = &f.stderr
			go func() {
				err := cmd.Run()
				code := 0
				state := model.JobSucceeded
				if err != nil {
					state = model.JobFailed
					code = 1
					if ee, ok := err.(*exec.ExitError); ok {
						code = ee.ExitCode()
					}
				}
				f.mu.Lock()
				f.job.State = state
				f.job.ExitCode = &code
				f.mu.Unlock()
				// Ending the far side's output is what ends the client's
				// output stream, exactly as a finished job does.
				outW.Close()
			}()

			w.WriteHeader(http.StatusCreated)
			f.mu.Lock()
			job := f.job
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(job)

		case r.URL.Path == "/v1/jobs/j1":
			f.mu.Lock()
			job := f.job
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(server.JobView{Job: job})

		case r.URL.Path == "/v1/jobs/j1/logs":
			f.mu.Lock()
			b := f.stderr.String()
			f.mu.Unlock()
			_, _ = io.WriteString(w, b)

		case r.URL.Path == "/v1/jobs/j1/tty/out" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			rc.Flush()
			buf := make([]byte, 32*1024)
			for {
				n, err := outR.Read(buf)
				if n > 0 {
					if _, werr := w.Write(buf[:n]); werr != nil {
						return
					}
					rc.Flush()
				}
				if err != nil {
					return
				}
			}

		case r.URL.Path == "/v1/jobs/j1/tty/in" && r.Method == http.MethodPost:
			_ = rc.EnableFullDuplex()
			w.WriteHeader(http.StatusOK)
			rc.Flush()
			_, _ = io.Copy(inW, r.Body)
			inW.Close()

		default:
			http.NotFound(w, r)
		}
	}))

	t.Cleanup(func() { f.release(); f.ts.Close() })
	return f
}

func (f *farEnd) client() *client.Client { return client.New(f.ts.URL, "ctok") }

func cpCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// The basic promise: the bytes that left are the bytes that arrived.
func TestCopyToDeliversAFileIntact(t *testing.T) {
	f := newFarEnd(t)
	local, box := t.TempDir(), t.TempDir()

	src := filepath.Join(local, "train.py")
	body := []byte("import torch\nprint(torch.cuda.is_available())\n")
	require.NoError(t, os.WriteFile(src, body, 0o644))

	require.NoError(t, f.client().CopyTo(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Local: src, Remote: box + "/", Submitter: "agent-a",
	}))

	got, err := os.ReadFile(filepath.Join(box, "train.py"))
	require.NoError(t, err)
	require.Equal(t, body, got)
}

// The detail that makes this feature feel broken when it technically works:
// a script that lands non-executable. It costs one tar header field.
func TestCopyToPreservesTheExecutableBit(t *testing.T) {
	f := newFarEnd(t)
	local, box := t.TempDir(), t.TempDir()

	src := filepath.Join(local, "bench.sh")
	require.NoError(t, os.WriteFile(src, []byte("#!/bin/sh\necho hi\n"), 0o755))

	require.NoError(t, f.client().CopyTo(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Local: src, Remote: box + "/", Submitter: "agent-a",
	}))

	fi, err := os.Stat(filepath.Join(box, "bench.sh"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), fi.Mode().Perm(),
		"the copied script is not executable; whoever runs it gets 'permission denied' and blames the box")
}

func TestCopyToCopiesADirectoryRecursively(t *testing.T) {
	f := newFarEnd(t)
	local, box := t.TempDir(), t.TempDir()

	tree := filepath.Join(local, "project")
	require.NoError(t, os.MkdirAll(filepath.Join(tree, "configs", "deep"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tree, "main.py"), []byte("main"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tree, "configs", "a.yaml"), []byte("a: 1"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tree, "configs", "deep", "b.yaml"), []byte("b: 2"), 0o644))

	require.NoError(t, f.client().CopyTo(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Local: tree, Remote: box + "/", Submitter: "agent-a",
	}))

	for name, want := range map[string]string{
		"project/main.py":             "main",
		"project/configs/a.yaml":      "a: 1",
		"project/configs/deep/b.yaml": "b: 2",
	} {
		got, err := os.ReadFile(filepath.Join(box, name))
		require.NoErrorf(t, err, "%s did not arrive", name)
		require.Equal(t, want, string(got))
	}
}

// A symlink inside the source tree must not be a way to read something
// outside it. Following one would let a link named "data" quietly ship
// /etc/shadow, or ~/.ssh/id_ed25519, to a GPU box.
func TestCopyToDoesNotFollowASymlinkOutOfTheTree(t *testing.T) {
	f := newFarEnd(t)
	local, outside, box := t.TempDir(), t.TempDir(), t.TempDir()

	secret := filepath.Join(outside, "id_ed25519")
	require.NoError(t, os.WriteFile(secret, []byte("PRIVATE-KEY-MATERIAL"), 0o600))

	tree := filepath.Join(local, "project")
	require.NoError(t, os.MkdirAll(tree, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tree, "main.py"), []byte("main"), 0o644))
	require.NoError(t, os.Symlink(secret, filepath.Join(tree, "data")))

	require.NoError(t, f.client().CopyTo(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Local: tree, Remote: box + "/", Submitter: "agent-a",
	}))

	// The rest of the tree still arrives — refusing the whole copy over one
	// link would be its own kind of broken.
	got, err := os.ReadFile(filepath.Join(box, "project", "main.py"))
	require.NoError(t, err)
	require.Equal(t, "main", string(got))

	// The link arrived as a link, and its target's CONTENTS did not.
	fi, err := os.Lstat(filepath.Join(box, "project", "data"))
	require.NoError(t, err)
	require.NotZero(t, fi.Mode()&os.ModeSymlink, "the symlink was followed instead of archived")

	var found bytes.Buffer
	require.NoError(t, filepath.Walk(box, func(p string, info os.FileInfo, err error) error {
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		b, _ := os.ReadFile(p)
		found.Write(b)
		return nil
	}))
	require.NotContains(t, found.String(), "PRIVATE-KEY-MATERIAL",
		"the contents of a file outside the source tree were shipped to the box")
}

// A copy runs under a lease exactly like any other execution. There is no
// unauthenticated upload path and no way to write to a box someone else is
// using — and the refusal has to name them, or the operator has no idea
// whether to wait or to ask.
func TestCopyToADeviceSomeoneElseHoldsIsRefused(t *testing.T) {
	f := newFarEnd(t)
	f.holder = "agent-b"
	local, box := t.TempDir(), t.TempDir()

	src := filepath.Join(local, "train.py")
	require.NoError(t, os.WriteFile(src, []byte("x"), 0o644))

	err := f.client().CopyTo(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Local: src, Remote: box + "/", Submitter: "agent-a",
	})
	require.ErrorIs(t, err, client.ErrDeviceHeldByAnother)
	require.Contains(t, err.Error(), "agent-b", "the refusal must say who has it")

	// And nothing was submitted, so no lease was even attempted.
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Empty(t, f.submitted.Command, "a refused copy still submitted a job")

	_, statErr := os.Stat(filepath.Join(box, "train.py"))
	require.Error(t, statErr, "a refused copy still wrote to the box")
}

// The mirror, with the same two properties that matter: the bytes and the
// mode.
func TestCopyFromFetchesAFileWithItsMode(t *testing.T) {
	f := newFarEnd(t)
	box, local := t.TempDir(), t.TempDir()

	body := []byte("{\"loss\": 0.31}\n")
	require.NoError(t, os.WriteFile(filepath.Join(box, "result.json"), body, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(box, "rerun.sh"), []byte("#!/bin/sh\n"), 0o755))

	c := f.client()
	require.NoError(t, c.CopyFrom(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Remote: filepath.Join(box, "result.json"), Local: local + "/", Submitter: "agent-a",
	}))
	got, err := os.ReadFile(filepath.Join(local, "result.json"))
	require.NoError(t, err)
	require.Equal(t, body, got)

	f2 := newFarEnd(t)
	require.NoError(t, f2.client().CopyFrom(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Remote: filepath.Join(box, "rerun.sh"), Local: local + "/", Submitter: "agent-a",
	}))
	fi, err := os.Stat(filepath.Join(local, "rerun.sh"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), fi.Mode().Perm(),
		"a fetched script lost its executable bit on the way back")
}

func TestCopyFromFetchesADirectoryRecursively(t *testing.T) {
	f := newFarEnd(t)
	box, local := t.TempDir(), t.TempDir()

	tree := filepath.Join(box, "out")
	require.NoError(t, os.MkdirAll(filepath.Join(tree, "ckpt"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tree, "log.txt"), []byte("log"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tree, "ckpt", "step1.pt"), []byte("weights"), 0o644))

	require.NoError(t, f.client().CopyFrom(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Remote: tree, Local: local + "/", Submitter: "agent-a",
	}))

	got, err := os.ReadFile(filepath.Join(local, "out", "ckpt", "step1.pt"))
	require.NoError(t, err)
	require.Equal(t, "weights", string(got))
}

func TestCopyFromADeviceSomeoneElseHoldsIsRefused(t *testing.T) {
	f := newFarEnd(t)
	f.holder = "agent-b"
	box, local := t.TempDir(), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(box, "secret.txt"), []byte("theirs"), 0o644))

	err := f.client().CopyFrom(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Remote: filepath.Join(box, "secret.txt"), Local: local + "/", Submitter: "agent-a",
	})
	require.ErrorIs(t, err, client.ErrDeviceHeldByAnother)
	require.Contains(t, err.Error(), "agent-b")
}

// A failing far side has to say why. `tar` writes its complaint to stderr,
// which in pipe mode is the job's log — stdout is the payload and cannot be
// shared with diagnostics — so a missing file must come back as the message
// tar actually printed, not a bare exit code.
func TestCopyFromAMissingFileReportsWhatTarSaid(t *testing.T) {
	f := newFarEnd(t)
	box, local := t.TempDir(), t.TempDir()

	err := f.client().CopyFrom(cpCtx(t), client.CopyOptions{
		DeviceID: "dgx:gpu0", Remote: filepath.Join(box, "nope.txt"), Local: local + "/", Submitter: "agent-a",
	})
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "nope.txt")
}

// The direction the other one cannot cover: the archive comes off a machine
// this client does not get to trust. An entry that cleans out of the
// destination — ../../.ssh/authorized_keys — is a way to take over whatever
// is running the copy, and "we ran their tar and it was fine" is not a
// security model. Built by hand, because no cooperating far end would send
// one.
func TestExtractingAnArchiveThatEscapesTheDestinationIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, entry string }{
		{"parent traversal", "../../.ssh/authorized_keys"},
		{"traversal in the middle", "out/../../../.ssh/authorized_keys"},
		{"absolute path", "/etc/cron.d/pwn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dest := t.TempDir()
			outside := filepath.Join(dest, "..", ".ssh")

			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			body := []byte("ssh-ed25519 AAAA... attacker\n")
			require.NoError(t, tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg, Name: tc.entry, Mode: 0o600, Size: int64(len(body)),
			}))
			_, err := tw.Write(body)
			require.NoError(t, err)
			require.NoError(t, tw.Close())

			err = client.ExtractTarForTest(&buf, dest, "")
			require.Error(t, err, "an archive entry that escapes the destination was extracted")
			require.Contains(t, err.Error(), tc.entry)

			_, statErr := os.Stat(filepath.Join(outside, "authorized_keys"))
			require.Error(t, statErr, "the escaping entry was written outside the destination")
		})
	}
}

// The same escape, one indirection further out: a symlink inside the archive
// pointing at /etc, followed by an entry that writes "through" it. GNU tar
// has had CVEs for exactly this shape.
func TestExtractingASymlinkOutOfTheDestinationIsRefused(t *testing.T) {
	dest := t.TempDir()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeSymlink, Name: "escape", Linkname: "../../../../etc", Mode: 0o777,
	}))
	require.NoError(t, tw.Close())

	err := client.ExtractTarForTest(&buf, dest, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "escape")
}

// An ordinary relative symlink inside the archive is not an attack and must
// still arrive, or "reject anything with a link in it" would pass the test
// above while making the feature useless.
func TestExtractingAnInnocentSymlinkWorks(t *testing.T) {
	dest := t.TempDir()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeSymlink, Name: "latest", Linkname: "step1.pt", Mode: 0o777,
	}))
	require.NoError(t, tw.Close())

	require.NoError(t, client.ExtractTarForTest(&buf, dest, ""))
	target, err := os.Readlink(filepath.Join(dest, "latest"))
	require.NoError(t, err)
	require.Equal(t, "step1.pt", target)
}
