package store_test

import (
	"testing"

	"github.com/mudler/resource-controller/internal/model"
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

// TestAllLabelsGroupsByDeviceAcrossTheWholeFleet is AllLabels' own coverage:
// deviceViews (behind /v1/state, /v1/devices, and the dashboard) calls it
// once for every device rather than calling LabelsFor per device, so it must
// actually group rows by device correctly, not just run the query. Two
// devices, both sources on one of them, proves the grouping keeps every
// device's rows together without mixing them into another device's slice.
func TestAllLabelsGroupsByDeviceAcrossTheWholeFleet(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia", "vram": "80G"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDeclared,
		map[string]string{"rack": "r3"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu1", model.SourceDetected,
		map[string]string{"vendor": "amd"}, c.Now()))

	all, err := s.AllLabels()
	require.NoError(t, err)

	require.Len(t, all, 2, "expected exactly the two devices that have labels")
	require.Len(t, all["gpubox:gpu0"], 3, "gpu0's two detected keys plus its one declared key")
	require.Len(t, all["gpubox:gpu1"], 1)

	var gpu1Vendor *model.Label
	for i := range all["gpubox:gpu1"] {
		if all["gpubox:gpu1"][i].Key == "vendor" {
			gpu1Vendor = &all["gpubox:gpu1"][i]
		}
	}
	require.NotNil(t, gpu1Vendor)
	require.Equal(t, "amd", gpu1Vendor.Value,
		"gpu1's own label must never pick up a value from gpu0's rows")
	require.Equal(t, model.SourceDetected, gpu1Vendor.Source)
}

// TestAllLabelsOnAFleetWithNoLabelsIsEmptyNotError is the boundary AllLabels
// exists to serve first: a controller that has just started, before any
// worker has pushed a single label, must not turn deviceViews into a 500.
func TestAllLabelsOnAFleetWithNoLabelsIsEmptyNotError(t *testing.T) {
	s, _ := newStore(t)

	all, err := s.AllLabels()
	require.NoError(t, err)
	require.Empty(t, all)
}
