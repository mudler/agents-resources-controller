package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// userConfig is the per-user client configuration, read from
// ~/.config/rc/config.yaml so that a plain shell can talk to the controller
// without sourcing an env file first. Environment variables still win, which
// keeps CI and one-off overrides working unchanged.
type userConfig struct {
	Controller string `yaml:"controller"`
	Token      string `yaml:"token"`
	Submitter  string `yaml:"submitter"`
}

var (
	userConfigOnce sync.Once
	userConfigVal  userConfig
	userConfigErr  error
)

// resetUserConfig clears the memoised config. Tests only: the config is read
// once per process in normal use.
func resetUserConfig() {
	userConfigOnce = sync.Once{}
	userConfigVal = userConfig{}
	userConfigErr = nil
}

// userConfigPath returns the config file location. RC_CONFIG overrides it,
// which is what the tests use and what lets a worker or a container point at
// a different file.
func userConfigPath() (string, error) {
	if v := strings.TrimSpace(os.Getenv("RC_CONFIG")); v != "" {
		return v, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "rc", "config.yaml"), nil
}

// loadUserConfig reads and memoises the config file. A missing file is not an
// error — it is the normal state for anyone driving rc purely from the
// environment. A file that exists but cannot be parsed IS an error, because
// silently ignoring it produces the confusing "unauthorized" that this
// feature exists to prevent.
func loadUserConfig() (userConfig, error) {
	userConfigOnce.Do(func() {
		path, err := userConfigPath()
		if err != nil {
			userConfigErr = err
			return
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				userConfigErr = fmt.Errorf("reading %s: %w", path, err)
			}
			return
		}
		var cfg userConfig
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			userConfigErr = fmt.Errorf("parsing %s: %w", path, err)
			return
		}
		cfg.Controller = strings.TrimSpace(cfg.Controller)
		cfg.Token = strings.TrimSpace(cfg.Token)
		cfg.Submitter = strings.TrimSpace(cfg.Submitter)
		userConfigVal = cfg
	})
	return userConfigVal, userConfigErr
}

// userConfigValue returns one field of the config, reporting a parse failure
// once on stderr rather than failing the command outright. A broken config
// should be loud, but it should not stop a caller who has the environment set.
var warnOnce sync.Once

func userConfigValue(pick func(userConfig) string) string {
	cfg, err := loadUserConfig()
	if err != nil {
		warnOnce.Do(func() {
			fmt.Fprintf(os.Stderr, "rc: warning: %v\n", err)
		})
		return ""
	}
	return pick(cfg)
}
