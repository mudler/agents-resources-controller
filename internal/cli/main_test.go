package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMain points RC_CONFIG at a path that cannot exist, for every test in
// this package.
//
// Without it the suite reads the developer's real ~/.config/rc/config.yaml.
// That is not hypothetical: adding the config-file fallback made
// TestDefaultSubmitterFallsBackToUserHostSession pass on a machine with no
// config and fail on the same commit the moment one was written, because the
// file supplied a submitter the test expected to be absent. A test whose
// result depends on the home directory of whoever runs it is worse than no
// test — it is green in CI and red for one person, or the reverse.
//
// Tests that DO want a config still set RC_CONFIG themselves (see
// writeUserConfig in userconfig_test.go); an explicit t.Setenv overrides this.
func TestMain(m *testing.M) {
	if err := os.Setenv("RC_CONFIG", filepath.Join(os.TempDir(), "rc-tests-must-not-read-a-real-config", "absent.yaml")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
