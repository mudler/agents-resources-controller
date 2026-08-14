package worker

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ControllerURL     string         `yaml:"controller_url"`
	Token             string         `yaml:"token"`
	Host              string         `yaml:"host"`
	Devices           []DeviceConfig `yaml:"devices"`
	HeartbeatInterval time.Duration  `yaml:"heartbeat_interval"`
	PollWait          time.Duration  `yaml:"poll_wait"`
}

// DeviceConfig is one device this host offers. It accepts either a bare name
// (stage 1 style) or an object with a runtime ceiling.
type DeviceConfig struct {
	Name       string        `yaml:"name"`
	MaxRuntime time.Duration `yaml:"max_runtime"`
}

func (d *DeviceConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		return value.Decode(&d.Name)
	}
	type plain DeviceConfig // avoid recursing into this method
	var p plain
	if err := value.Decode(&p); err != nil {
		return err
	}
	*d = DeviceConfig(p)
	if d.Name == "" {
		return fmt.Errorf("device entry needs a name")
	}
	return nil
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
	for _, d := range c.Devices {
		if d.Name == "" {
			return Config{}, fmt.Errorf("device entry needs a name in %s", path)
		}
		// The wire ceiling and the devices.max_runtime column are both
		// seconds; a positive but sub-second value would truncate to 0 at
		// the server and be silently treated as "no ceiling" rather than
		// the tiny one written here. Reject it now, at the one point that
		// can still name what the operator actually wrote, instead of
		// letting it round away into a limit that never enforces.
		if d.MaxRuntime > 0 && d.MaxRuntime < time.Second {
			return Config{}, fmt.Errorf(
				"device %q: max_runtime %s is below the one-second granularity runtime ceilings are enforced at, in %s",
				d.Name, d.MaxRuntime, path)
		}
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.PollWait <= 0 {
		c.PollWait = 30 * time.Second
	}
	return c, nil
}
