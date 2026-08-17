package selector_test

import (
	"testing"

	"github.com/mudler/resource-controller/internal/selector"
	"github.com/stretchr/testify/require"
)

// An ordered comparison against a value that is not a number must not match.
//
// Found on the live fleet: nvidia-smi reports "[N/A]" for total memory on a
// GB10 (unified memory) and on a Thor, so both boxes carried vram=[N/A]M —
// and `vram>=40G` matched BOTH, because the fallback compared the strings
// and '[' (0x5B) sorts after '4' (0x34). An agent asking for 80G of VRAM was
// scheduled onto hardware whose VRAM is unknown.
//
// This is the same rule the absent-key case already follows: a fact we do not
// have is not evidence the device qualifies.
func TestOrderedComparisonRejectsANonNumericValue(t *testing.T) {
	for _, expr := range []string{"vram>=40G", "vram<=40G"} {
		s, err := selector.Parse(expr)
		require.NoError(t, err)
		require.False(t, s.Match(map[string]string{"vram": "[N/A]M"}),
			"%s must not match a device whose vram is not a number", expr)
	}
}

// The mirror: a numeric selector against a non-numeric device value is the
// case above; this is a non-numeric SELECTOR, which is equally unprovable.
func TestOrderedComparisonRejectsANonNumericSelectorValue(t *testing.T) {
	s, err := selector.Parse("vram>=lots")
	require.NoError(t, err)
	require.False(t, s.Match(map[string]string{"vram": "80G"}))
}

// Equality is unaffected: it is a string comparison by design, and matching
// "[N/A]M" exactly is a legitimate thing to ask for.
func TestEqualityStillComparesStrings(t *testing.T) {
	s, err := selector.Parse("vram=[N/A]M")
	require.NoError(t, err)
	require.True(t, s.Match(map[string]string{"vram": "[N/A]M"}))

	ne, err := selector.Parse("vram!=[N/A]M")
	require.NoError(t, err)
	require.False(t, ne.Match(map[string]string{"vram": "[N/A]M"}))
	require.True(t, ne.Match(map[string]string{"vram": "80G"}))
}

// And ordered comparisons still work when both sides are numbers.
func TestOrderedComparisonStillWorksOnNumbers(t *testing.T) {
	s, err := selector.Parse("vram>=40G")
	require.NoError(t, err)
	require.True(t, s.Match(map[string]string{"vram": "80G"}))
	require.False(t, s.Match(map[string]string{"vram": "24G"}))
}
