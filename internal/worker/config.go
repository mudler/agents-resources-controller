package worker

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultHookTimeout and defaultReleaseLinger are the documented fallbacks
// applied when neither the host-level "hooks:" section nor a device
// overrides them.
const (
	defaultHookTimeout   = 60 * time.Second
	defaultReleaseLinger = 30 * time.Second

	defaultHeartbeatInterval = 10 * time.Second
	defaultPollWait          = 30 * time.Second

	// defaultProbeDir, defaultProbeInterval, and defaultProbeTimeout are the
	// documented fallbacks for probing: where drop-in probes live, how often
	// a full pass re-runs, and how long any single probe (built-in
	// nvidia-smi call or drop-in executable) is allowed before it is killed
	// as a process group and skipped.
	defaultProbeDir      = "/etc/rc/probe.d"
	defaultProbeInterval = 5 * time.Minute
	defaultProbeTimeout  = 5 * time.Second

	// defaultSheetDir is where readSheets looks for host.md and
	// host.d/<device>.md when the operator does not set sheet_dir.
	defaultSheetDir = "/etc/rc"
)

type Config struct {
	ControllerURL     string         `yaml:"controller_url"`
	Token             string         `yaml:"token"`
	Host              string         `yaml:"host"`
	Devices           []DeviceConfig `yaml:"devices"`
	HeartbeatInterval time.Duration  `yaml:"heartbeat_interval"`
	PollWait          time.Duration  `yaml:"poll_wait"`
	// Hooks holds host-level defaults for the lifecycle hook timeout and
	// release linger, so a multi-GPU box need not repeat them on every
	// device. Both are optional; unset fields fall back to
	// defaultHookTimeout / defaultReleaseLinger.
	Hooks HooksConfig `yaml:"hooks"`
	// ProbeDir is where this worker looks for drop-in probe executables,
	// run in name order on top of the built-ins to gather device labels.
	// Optional; falls back to defaultProbeDir.
	ProbeDir string `yaml:"probe_dir"`
	// ProbeInterval is how often a full probe pass re-runs while the worker
	// is up, so a label picks up a change (a card swap, a driver upgrade)
	// without a restart. Optional; falls back to defaultProbeInterval.
	ProbeInterval time.Duration `yaml:"probe_interval"`
	// ProbeTimeout bounds any single probe — a built-in nvidia-smi call or
	// one drop-in executable — before it is killed as a process group and
	// skipped. Optional; falls back to defaultProbeTimeout.
	ProbeTimeout time.Duration `yaml:"probe_timeout"`
	// SheetDir is where this worker looks for its usage-sheet documentation:
	// <SheetDir>/host.md and <SheetDir>/host.d/<device>.md. Optional; falls
	// back to defaultSheetDir.
	SheetDir string `yaml:"sheet_dir"`
}

// HooksConfig is the host-level default for lease lifecycle hooks. A
// per-device value (DeviceConfig.HookTimeout / ReleaseLinger) overrides it.
type HooksConfig struct {
	Timeout       time.Duration `yaml:"timeout"`
	ReleaseLinger time.Duration `yaml:"release_linger"`
}

// DeviceConfig is one device this host offers. It accepts either a bare name
// (stage 1 style) or an object with a runtime ceiling and optional lease
// lifecycle hooks.
type DeviceConfig struct {
	Name       string        `yaml:"name"`
	MaxRuntime time.Duration `yaml:"max_runtime"`
	// OnAcquire and OnRelease are paths to scripts run (via the same
	// process-group supervision jobs get) when this device transitions
	// worker-side between free and held. Both are optional and
	// independent — either, neither, or both may be set.
	OnAcquire string `yaml:"on_acquire"`
	OnRelease string `yaml:"on_release"`
	// HookTimeout and ReleaseLinger override the host-level Hooks defaults
	// for this device only. Zero means "not overridden": LoadConfig
	// resolves each to the host default (or the built-in default, if the
	// host declared none) once loading is complete.
	HookTimeout   time.Duration `yaml:"timeout"`
	ReleaseLinger time.Duration `yaml:"release_linger"`
	// Labels are this device's declared facts — operator-asserted, as
	// opposed to what a probe detects — keyed the same flat way a probe's
	// output is: a bare key ("rack") or, though redundant for a per-device
	// list, a "<device-name>.<label>" key is accepted wherever labels are
	// merged. Optional.
	Labels map[string]string `yaml:"labels"`
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

// withDefaults resolves every documented fallback in a Config, whatever
// route that Config arrived by. LoadConfig calls it, and so does New — a
// Config built in code (an embedded worker, a test, a future `rc worker`
// flag path) must never end up with a zero hook timeout, because a zero
// timeout means no MaxRuntime at all, which means a wedged hook runs
// forever: the startup pass then blocks Start indefinitely and the worker
// never polls. "Unset" must always mean the documented default, never
// "unbounded".
//
// It is idempotent: applying it to an already-defaulted Config changes
// nothing, so LoadConfig calling it and then New calling it again is fine.
func (c Config) withDefaults() Config {
	hostHookTimeout := c.Hooks.Timeout
	if hostHookTimeout <= 0 {
		hostHookTimeout = defaultHookTimeout
	}
	hostReleaseLinger := c.Hooks.ReleaseLinger
	if hostReleaseLinger <= 0 {
		hostReleaseLinger = defaultReleaseLinger
	}
	c.Hooks.Timeout = hostHookTimeout
	c.Hooks.ReleaseLinger = hostReleaseLinger

	devices := make([]DeviceConfig, len(c.Devices))
	copy(devices, c.Devices) // never mutate the caller's slice
	for i := range devices {
		if devices[i].HookTimeout <= 0 {
			devices[i].HookTimeout = hostHookTimeout
		}
		if devices[i].ReleaseLinger <= 0 {
			devices[i].ReleaseLinger = hostReleaseLinger
		}
	}
	c.Devices = devices

	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.PollWait <= 0 {
		c.PollWait = defaultPollWait
	}
	if c.ProbeDir == "" {
		c.ProbeDir = defaultProbeDir
	}
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = defaultProbeInterval
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = defaultProbeTimeout
	}
	if c.SheetDir == "" {
		c.SheetDir = defaultSheetDir
	}
	return c
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
		// A hook path is optional, but if given it must actually name
		// something: a blank or whitespace-only value is almost certainly
		// an operator mistake (e.g. a stray quoted empty string), not an
		// intentional "no hook". The path is deliberately NOT stat'd here —
		// the script may legitimately not exist yet.
		if d.OnAcquire != "" && strings.TrimSpace(d.OnAcquire) == "" {
			return Config{}, fmt.Errorf("device %q: on_acquire must not be blank, in %s", d.Name, path)
		}
		if d.OnRelease != "" && strings.TrimSpace(d.OnRelease) == "" {
			return Config{}, fmt.Errorf("device %q: on_release must not be blank, in %s", d.Name, path)
		}
	}
	// Every documented fallback — hook timeout, release linger, heartbeat
	// interval, poll wait — lives in one place, applied here and again in
	// New for a Config that never came through this function.
	return c.withDefaults(), nil
}
