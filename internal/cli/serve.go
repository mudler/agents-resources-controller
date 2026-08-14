package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/spf13/cobra"
)

// HeartbeatGrace is how long a device's worker may go without heartbeating
// before it is treated as out of contact. It gates both the reaper's Sweep
// threshold below and rc devices' "no contact" annotation (internal/cli/ps.go)
// so the two thresholds cannot silently drift apart.
const HeartbeatGrace = 30 * time.Second

func NewServeCmd() *cobra.Command {
	var (
		addr    string
		dataDir string
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the controller",
		RunE: func(cmd *cobra.Command, args []string) error {
			tokens, err := loadTokens()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				return err
			}

			c := clock.Real()
			st, err := store.Open(filepath.Join(dataDir, "rc.db"), c)
			if err != nil {
				return err
			}
			defer st.Close()

			logs, err := logstore.New(filepath.Join(dataDir, "logs"))
			if err != nil {
				return err
			}

			srv := server.New(server.Config{Store: st, Logs: logs, Clock: c, Tokens: tokens})

			// The reaper: silent workers lose their devices to unknown, then
			// unhealthy. Nothing here ever promotes a device to ready.
			//
			// It runs on its own derived context, not cmd.Context() directly:
			// RunE can return along a path that never cancels cmd.Context() at
			// all — e.g. ListenAndServe failing immediately on a bind error —
			// and reaperWG.Wait() below must not block forever waiting for a
			// cancellation that will never come. cancelReaper is deferred so it
			// fires on every return path; reaperWG.Wait() is joined before
			// st.Close() runs (see the deferred order below) so the reaper can
			// never touch the store after it's closed and log a spurious
			// "sql: database is closed".
			reaperCtx, cancelReaper := context.WithCancel(cmd.Context())

			var reaperWG sync.WaitGroup
			reaperWG.Add(1)
			go func() {
				defer reaperWG.Done()
				t := time.NewTicker(10 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-reaperCtx.Done():
						return
					case <-t.C:
						res, err := st.Sweep(HeartbeatGrace, 5*time.Minute)
						if err != nil {
							slog.Error("sweep", "err", err)
							continue
						}
						if len(res.DevicesUnhealthy) > 0 || len(res.JobsLost) > 0 || len(res.LeasesExpired) > 0 {
							slog.Warn("devices demoted",
								"unhealthy", res.DevicesUnhealthy, "jobs_lost", res.JobsLost,
								"leases_expired", res.LeasesExpired)
						}
					}
				}
			}()
			// Deferred in this order so Go's LIFO unwind runs them
			// cancelReaper -> reaperWG.Wait -> st.Close: stop the reaper first
			// (works even if cmd.Context() is still live), then wait for it to
			// actually exit, then close the store it was using.
			defer reaperWG.Wait()
			defer cancelReaper()

			httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}
			go func() {
				<-cmd.Context().Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = httpSrv.Shutdown(shutdownCtx)
			}()

			slog.Info("controller listening", "addr", addr, "data", dataDir)
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	cmd.Flags().StringVar(&dataDir, "data", "/var/lib/rc", "state directory")
	return cmd
}

// loadTokens reads RC_TOKENS as "token:role,token:role". It rejects anything
// that would make the resulting policy something other than what the
// operator literally wrote: a malformed entry, an unknown role, an empty
// token, or a token repeated with a second (possibly different, possibly
// higher-privileged) role. A silently-dropped or silently-overwritten entry
// is an auth policy nobody actually reviewed.
func loadTokens() (map[string]string, error) {
	raw := os.Getenv("RC_TOKENS")
	if raw == "" {
		return nil, fmt.Errorf("RC_TOKENS required, e.g. RC_TOKENS='wtok:worker,ctok:client,atok:admin'")
	}
	tokens := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		token, role, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok {
			return nil, fmt.Errorf("malformed RC_TOKENS entry %q", pair)
		}
		if token == "" {
			return nil, fmt.Errorf("empty token in RC_TOKENS entry %q", pair)
		}
		switch role {
		case "worker", "client", "admin":
		default:
			return nil, fmt.Errorf("unknown role %q in RC_TOKENS", role)
		}
		if _, dup := tokens[token]; dup {
			return nil, fmt.Errorf("duplicate token %q in RC_TOKENS", token)
		}
		tokens[token] = role
	}
	return tokens, nil
}
