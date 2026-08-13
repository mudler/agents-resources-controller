package worker

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ControllerURL     string        `yaml:"controller_url"`
	Token             string        `yaml:"token"`
	Host              string        `yaml:"host"`
	Devices           []string      `yaml:"devices"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	PollWait          time.Duration `yaml:"poll_wait"`
}

// LoadConfig reads /etc/rc/worker.yaml (or another path) and applies defaults.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read worker config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return Config{}, fmt.Errorf("parse worker config: %w", err)
	}
	if c.Host == "" {
		h, err := os.Hostname()
		if err != nil {
			return Config{}, err
		}
		c.Host = h
	}
	if c.ControllerURL == "" {
		return Config{}, fmt.Errorf("controller_url required in %s", path)
	}
	if len(c.Devices) == 0 {
		return Config{}, fmt.Errorf("at least one device required in %s", path)
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.PollWait <= 0 {
		c.PollWait = 30 * time.Second
	}
	return c, nil
}
