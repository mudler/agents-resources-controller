package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/spf13/cobra"
)

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
			go func() {
				t := time.NewTicker(10 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-cmd.Context().Done():
						return
					case <-t.C:
						res, err := st.Sweep(30*time.Second, 5*time.Minute)
						if err != nil {
							slog.Error("sweep", "err", err)
							continue
						}
						if len(res.DevicesUnhealthy) > 0 || len(res.JobsLost) > 0 {
							slog.Warn("devices demoted",
								"unhealthy", res.DevicesUnhealthy, "jobs_lost", res.JobsLost)
						}
					}
				}
			}()

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

// loadTokens reads RC_TOKENS as "token:role,token:role".
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
		switch role {
		case "worker", "client", "admin":
		default:
			return nil, fmt.Errorf("unknown role %q in RC_TOKENS", role)
		}
		tokens[token] = role
	}
	return tokens, nil
}
