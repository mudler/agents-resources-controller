package selector_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/selector"
	"github.com/stretchr/testify/require"
)

func TestParseAndMatchEquality(t *testing.T) {
	sel, err := selector.Parse("vendor=nvidia")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vendor": "nvidia"}))
	require.False(t, sel.Match(map[string]string{"vendor": "amd"}))
	require.False(t, sel.Match(map[string]string{}), "a missing key never matches")
}

func TestParseAndMatchInequality(t *testing.T) {
	sel, err := selector.Parse("vendor!=amd")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vendor": "nvidia"}))
	require.False(t, sel.Match(map[string]string{"vendor": "amd"}))
	require.False(t, sel.Match(map[string]string{}),
		"a missing key does not satisfy != either: we cannot prove it")
}

func TestNumericComparisonWithSuffixes(t *testing.T) {
	sel, err := selector.Parse("vram>=40G")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vram": "80G"}))
	require.True(t, sel.Match(map[string]string{"vram": "40G"}))
	require.False(t, sel.Match(map[string]string{"vram": "24G"}))
	require.True(t, sel.Match(map[string]string{"vram": "81920M"}))
}

func TestConjunction(t *testing.T) {
	sel, err := selector.Parse("vendor=nvidia,vram>=40G")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vendor": "nvidia", "vram": "80G"}))
	require.False(t, sel.Match(map[string]string{"vendor": "nvidia", "vram": "24G"}))
	require.False(t, sel.Match(map[string]string{"vram": "80G"}))
}

func TestStringComparisonWhenNotNumeric(t *testing.T) {
	sel, err := selector.Parse("model>=a100")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"model": "h100"}))
	require.False(t, sel.Match(map[string]string{"model": "a10"}))
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "   ", "vendor", "=nvidia", "vendor=", ",", "a=b,,c=d", "a>=b>=c", "a=b=c", "a<=b<=c", "a!=b!=c"} {
		_, err := selector.Parse(in)
		require.Error(t, err, "input %q should not parse", in)
	}
}

func TestStringRoundTrips(t *testing.T) {
	sel, err := selector.Parse(" vendor = nvidia , vram >= 40G ")
	require.NoError(t, err)
	require.Equal(t, "vendor=nvidia,vram>=40G", sel.String(),
		"whitespace is normalised so an equivalent selector has one representation")
}
