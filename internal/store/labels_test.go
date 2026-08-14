package store_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

func TestReplaceLabelsStoresProvenanceAndTime(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia", "vram": "80G"}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 2)
	for _, l := range labels {
		require.Equal(t, model.SourceDetected, l.Source)
		require.Equal(t, c.Now(), l.UpdatedAt)
	}
}

// Replacing a source drops the keys that source no longer reports — a card
// that vanished must not leave its VRAM label behind.
func TestReplaceLabelsRemovesKeysThatSourceNoLongerReports(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia", "vram": "80G"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia"}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 1)
	require.Equal(t, "vendor", labels[0].Key)
}

// The two sources coexist so the conflict stays visible in rc describe.
func TestDeclaredAndDetectedCoexist(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vram": "80G"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDeclared,
		map[string]string{"vram": "40G"}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 2, "both rows survive")
}

// ...but the scheduler uses one value, and detection wins.
func TestSnapshotPrefersDetectedOverDeclared(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDeclared,
		map[string]string{"vram": "40G", "owner": "team-a"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vram": "80G"}, c.Now()))

	snap, err := s.LabelSnapshot()
	require.NoError(t, err)
	require.Equal(t, "80G", snap["gpubox:gpu0"]["vram"],
		"a declared value must never override a detected one")
	require.Equal(t, "team-a", snap["gpubox:gpu0"]["owner"],
		"declared labels still contribute keys detection does not report")
}

func TestReplaceLabelsWithEmptyMapClearsThatSource(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Empty(t, labels)
}

func TestLabelSnapshotCoversEveryDevice(t *testing.T) {
	s, c := newStore(t)
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia"}, c.Now()))

	snap, err := s.LabelSnapshot()
	require.NoError(t, err)
	require.Contains(t, snap, "gpubox:gpu0")
}
