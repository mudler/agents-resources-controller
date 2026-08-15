package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/cli"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// iptr is the *int twin of server.ptr's *string: DescribeResponse.
// SheetAgeSeconds is a pointer specifically so a present, explicit zero can
// be told apart from an absent field (an older controller that predates
// this field) — every test that wants a real age has to build one of these
// rather than write a bare int literal.
func iptr(i int) *int { return &i }

// describeServer stands up a stub controller answering exactly one describe
// response for gpubox:gpu0, so each test only has to build the response it
// cares about.
func describeServer(t *testing.T, resp server.DescribeResponse) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/devices/gpubox:gpu0/describe" {
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func runDescribe(t *testing.T, ts *httptest.Server, args ...string) (string, error) {
	t.Helper()
	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewDescribeCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs(append([]string{"gpubox:gpu0"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// sectionBetween returns the text strictly between two section headings, so
// an assertion can be scoped to exactly the section it claims to test
// instead of being satisfiable by an unrelated line elsewhere in the
// output (e.g. the heartbeat line, which also ends in "...ago"). Fails the
// test outright if either heading is missing, since a caller asking for a
// section that isn't there is a test bug, not a soft signal.
func sectionBetween(t *testing.T, out, startHeading, endHeading string) string {
	t.Helper()
	start := strings.Index(out, startHeading)
	require.GreaterOrEqualf(t, start, 0, "missing heading %q in output:\n%s", startHeading, out)
	rest := out[start+len(startHeading):]
	end := strings.Index(rest, endHeading)
	require.GreaterOrEqualf(t, end, 0, "missing heading %q in output:\n%s", endHeading, out)
	return rest[:end]
}

// TestDescribeRendersLabelWithSourceAndAge pins the design's own example
// verbatim: a label must show its value, its source, and a relative age —
// computed by the controller (LabelAgeSeconds), not re-derived from
// UpdatedAt against the CLI process's own clock.
func TestDescribeRendersLabelWithSourceAndAge(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-4 * time.Minute)},
		},
		LabelAgeSeconds: map[string]int{"vram/" + model.SourceDetected: 4 * 60},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	require.Contains(t, out, "vram")
	require.Contains(t, out, "80G")
	require.Contains(t, out, "detected")
	require.Contains(t, out, "4m ago", "the label's age must be rendered as a relative age, not a raw timestamp")
}

// TestDescribeLabelAgeComesFromServerNotLocalClock is the falsifiable core
// of this fix: it builds a label stamped only a minute old but gives it a
// server-computed age of three hours — a combination that cannot arise from
// the CLI computing time.Now().Sub(UpdatedAt) locally, only from actually
// reading LabelAgeSeconds. If the CLI still computed the age from the
// timestamp (as it did before this change), this would render "1m ago" and
// the assertion below would fail.
//
// Verified failing first: before the CLI was switched to read
// LabelAgeSeconds, this test failed with output containing "1m ago" and not
// "3h ago".
func TestDescribeLabelAgeComesFromServerNotLocalClock(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-1 * time.Minute)},
		},
		LabelAgeSeconds: map[string]int{"vram/" + model.SourceDetected: 3 * 60 * 60},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	section := sectionBetween(t, out, "LABELS", "USAGE SHEET")
	require.Contains(t, section, "3h ago",
		"the rendered age must be the server's LabelAgeSeconds, not derived from the timestamp locally")
	require.NotContains(t, section, "1m ago",
		"a locally-derived age from the (much fresher) timestamp must not appear")
}

// TestDescribeShowsConflictingDeclaredValueBesideDetected is the guard
// against exactly the failure mode the brief warns about: an operator's
// hand-declared vram figure that disagrees with what was actually detected
// must be visible, not silently dropped in favor of the "winning" value the
// scheduler uses.
func TestDescribeShowsConflictingDeclaredValueBesideDetected(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-4 * time.Minute)},
			{Key: "vram", Value: "40G", Source: model.SourceDeclared, UpdatedAt: time.Now().Add(-9 * 24 * time.Hour)},
		},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	require.Contains(t, out, "80G", "the detected value must be shown")
	require.Contains(t, out, "40G", "the conflicting declared value must be shown, not hidden behind the winning detected one")
	require.Contains(t, out, "declared")
	require.Contains(t, out, "detected")
}

// TestDescribeShowsStaleSheetAge pins the "a sheet last edited in March"
// scenario from the design: an old sheet's age must actually read as old,
// not be silently rounded away or omitted because it's inconvenient.
//
// The assertion is scoped to the USAGE SHEET section specifically (via
// sectionBetween), not the whole rendered output: an earlier version of
// this test asserted Contains(out, "ago") and NotContains(out, "4m ago")
// against the FULL output, both of which the unrelated "heartbeat 0s ago"
// line already satisfies — a mutation that deleted the sheet's age line
// entirely left every assertion in this test green. Verified: temporarily
// no-op'ing renderSheet's `fmt.Fprintf(w, "  %s, updated %s\n", ...)` call
// now fails this test (it did not fail the old version).
func TestDescribeShowsStaleSheetAge(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device:          model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Sheet:           "shared rack A1, ask #infra before recabling",
		SheetUpdatedAt:  time.Now().Add(-100 * 24 * time.Hour),
		SheetAgeSeconds: iptr(100 * 24 * 60 * 60),
		SheetIsHostWide: false,
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)

	section := sectionBetween(t, out, "USAGE SHEET", "RECENT JOBS")
	require.Contains(t, section, "shared rack A1")
	require.Contains(t, section, "updated", "the sheet's own line must report when it was updated")
	require.Contains(t, section, "3mo ago",
		"a sheet last edited ~100 days ago must show its own real age, not the request time")
	require.NotContains(t, section, "0s ago")
	require.Contains(t, section, "device note", "a device-specific sheet must be labelled as such, not left ambiguous with a host-wide one")
}

// TestDescribeSheetAgeComesFromServerNotLocalClock is the sheet-side twin of
// TestDescribeLabelAgeComesFromServerNotLocalClock: SheetUpdatedAt says one
// minute ago, but SheetAgeSeconds says three hours — a combination only
// possible if the CLI reads SheetAgeSeconds rather than recomputing
// now.Sub(SheetUpdatedAt). Verified failing first the same way: before the
// switch, this rendered "1m ago", not "3h ago".
func TestDescribeSheetAgeComesFromServerNotLocalClock(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device:          model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Sheet:           "shared rack A1, ask #infra before recabling",
		SheetUpdatedAt:  time.Now().Add(-1 * time.Minute),
		SheetAgeSeconds: iptr(3 * 60 * 60),
		SheetIsHostWide: false,
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)

	section := sectionBetween(t, out, "USAGE SHEET", "RECENT JOBS")
	require.Contains(t, section, "3h ago",
		"the rendered sheet age must be the server's SheetAgeSeconds, not derived from SheetUpdatedAt locally")
	require.NotContains(t, section, "1m ago",
		"a locally-derived age from the (much fresher) timestamp must not appear")
}

// TestDescribeFutureStampedLabelIsNotRenderedAsFresh pins the other defect
// the brief calls out: a label whose UpdatedAt is ahead of the controller's
// clock produces a NEGATIVE LabelAgeSeconds. The old `d < 0 { d = 0 }` clamp
// would render that as "0s ago" — indistinguishable from the freshest
// possible label, when it is actually the least trustworthy one on the
// page (a clock disagreement, not a recent confirmation). The rendering
// must say so, not go quiet about it.
//
// The assertion is the exact phrase, not a loose Contains("future"): an
// earlier version of formatAge built this case by appending " in the
// future (clock skew?)" onto its OWN "...ago" output, rendering the
// self-contradictory "5m ago in the future (clock skew?)" — which also
// contains "future", so a looser assertion here would not have caught it.
func TestDescribeFutureStampedLabelIsNotRenderedAsFresh(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(5 * time.Minute)},
		},
		LabelAgeSeconds: map[string]int{"vram/" + model.SourceDetected: -5 * 60},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	section := sectionBetween(t, out, "LABELS", "USAGE SHEET")
	require.Contains(t, section, "5m in the future (clock skew?)",
		"a future-stamped label's anomaly must be visible, worded so it never also reads as an age")
	require.NotContains(t, section, "ago",
		"the future-stamped phrasing must not also claim an age, which is what made the pre-fix wording self-contradictory")
}

// TestDescribeFutureStampedSheetIsNotRenderedAsFresh is the sheet-side twin
// of TestDescribeFutureStampedLabelIsNotRenderedAsFresh. Its absence was a
// real gap: with the OLD (self-contradictory) "%s ago in the future"
// wording, a loose require.Contains(section, "3h ago") assertion here would
// have passed on BOTH the correct rendering and a regression that
// reintroduced the `d < 0 { d = 0 }` clamp on the server (silently making
// SheetAgeSeconds 0), because "3h ago" was never checked to be the WHOLE
// story. This pins the exact phrase, closing that hole.
func TestDescribeFutureStampedSheetIsNotRenderedAsFresh(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device:          model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Sheet:           "shared rack A1, ask #infra before recabling",
		SheetUpdatedAt:  time.Now().Add(10 * time.Minute),
		SheetAgeSeconds: iptr(-10 * 60),
		SheetIsHostWide: false,
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	section := sectionBetween(t, out, "USAGE SHEET", "RECENT JOBS")
	require.Contains(t, section, "10m in the future (clock skew?)",
		"a future-stamped sheet's anomaly must be visible, worded so it never also reads as an age")
	require.NotContains(t, section, "ago")
}

// TestDescribeLabelAgeUnknownWhenServerOmitsIt is FIX 1's load-bearing
// case: a label with a real, 90-day-old UpdatedAt but NO entry in
// LabelAgeSeconds — the shape an older controller (one that predates this
// field) sends a newer CLI. Before this fix, ages[key] on a plain lookup
// returns the map's zero value, 0, for a missing key exactly as it would
// for a real, explicit 0 — rendering "0s ago" for a 90-day-old fact. That
// is this task's own "a month-old label reads as fresh" defect, just
// triggered by version skew instead of clock skew.
func TestDescribeLabelAgeUnknownWhenServerOmitsIt(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-90 * 24 * time.Hour)},
		},
		// LabelAgeSeconds deliberately omitted (nil map): this is the shape
		// json.Decode produces for a controller response that never had this
		// field at all, not a device with zero labels.
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	section := sectionBetween(t, out, "LABELS", "USAGE SHEET")
	require.NotContains(t, section, "0s ago",
		"a label with no reported age must never render as the freshest possible reading")
	require.Contains(t, section, "age unknown",
		"a label with no reported age must say so explicitly")
}

// TestDescribeSheetAgeUnknownWhenServerOmitsIt is the sheet-side twin: a
// real, non-empty sheet with a genuine, hour-old SheetUpdatedAt but a nil
// SheetAgeSeconds (the same older-controller shape). Same defect, same fix:
// render an explicit unknown, never "0s ago".
func TestDescribeSheetAgeUnknownWhenServerOmitsIt(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device:         model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Sheet:          "shared rack A1, ask #infra before recabling",
		SheetUpdatedAt: time.Now().Add(-time.Hour),
		// SheetAgeSeconds deliberately left nil.
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	section := sectionBetween(t, out, "USAGE SHEET", "RECENT JOBS")
	require.NotContains(t, section, "0s ago",
		"a sheet with no reported age must never render as the freshest possible reading")
	require.Contains(t, section, "age unknown",
		"a sheet with no reported age must say so explicitly")
}

// TestDescribeShowsNoneOnFileForRealisticEmptySheet pins FIX 6: the shape a
// REAL worker sends for a host with no host.md at all is NOT "empty body,
// zero timestamp" — sheetPayload (internal/worker/worker.go) sends a
// non-nil pointer to "" in that case, which applyDeviceFacts stores as a
// real row with a real, non-zero timestamp. Gating "(none on file)" on
// updatedAt.IsZero(), as an earlier version of renderSheet did, meant that
// guard NEVER fired for this — the single most common real-world shape —
// and instead rendered a phantom "host-wide note, updated 1h ago" followed
// by a blank line. The guard must be body-only.
func TestDescribeShowsNoneOnFileForRealisticEmptySheet(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Sheet:  "",
		// A REAL, non-zero timestamp — exactly what a real worker's
		// registration leaves for a host with no host.md at all.
		SheetUpdatedAt:  time.Now().Add(-time.Hour),
		SheetAgeSeconds: nil,
		SheetIsHostWide: true,
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	section := sectionBetween(t, out, "USAGE SHEET", "RECENT JOBS")
	require.Contains(t, section, "(none on file)")
	require.NotContains(t, section, "updated",
		"an empty sheet must not render a phantom 'updated ... ago' line just because a real timestamp happens to be attached to its emptiness")
	require.NotContains(t, section, "host-wide note")
}

// TestDescribeLabelsHostWideSheetDistinctlyFromDeviceSheet pins the other
// half of the same distinction: when describe falls back to the host-wide
// note (no sheet of this device's own), the rendering must say so — an
// agent reading "don't run more than two jobs here" needs to know whether
// that rule is about the whole box or just this card.
func TestDescribeLabelsHostWideSheetDistinctlyFromDeviceSheet(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device:          model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Sheet:           "shared rack A1, ask #infra before recabling",
		SheetUpdatedAt:  time.Now().Add(-time.Hour),
		SheetIsHostWide: true,
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)

	section := sectionBetween(t, out, "USAGE SHEET", "RECENT JOBS")
	require.Contains(t, section, "host-wide")
	require.NotContains(t, section, "device note",
		"a host-wide fallback must not be mislabelled as this device's own note")
}

// TestDescribeJSONOutputParsesBackIntoDescribeResponse pins the machine-
// readable path: `-o json` must emit the response verbatim, round-trippable
// by any caller (an agent's own tooling) that wants the raw structure
// instead of the rendered text.
func TestDescribeJSONOutputParsesBackIntoDescribeResponse(t *testing.T) {
	want := server.DescribeResponse{
		Device:              model.Device{ID: "gpubox:gpu0", State: model.DeviceBusy},
		Holder:              "agent-a",
		JobID:               "job1",
		ElapsedSeconds:      42,
		HeartbeatAgeSeconds: 3,
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-4 * time.Minute).Truncate(time.Second)},
		},
		LabelAgeSeconds: map[string]int{"vram/" + model.SourceDetected: 240},
		Sheet:           "notes",
		SheetUpdatedAt:  time.Now().Add(-time.Hour).Truncate(time.Second),
		SheetAgeSeconds: iptr(3600),
		SheetIsHostWide: true,
		RecentJobs: []model.Job{
			{ID: "job0", DeviceID: "gpubox:gpu0", State: model.JobSucceeded, Command: []string{"./bench"}},
		},
	}
	ts := describeServer(t, want)

	out, err := runDescribe(t, ts, "-o", "json")
	require.NoError(t, err)

	var got server.DescribeResponse
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, want.Device, got.Device)
	require.Equal(t, want.Holder, got.Holder)
	require.Equal(t, want.JobID, got.JobID)
	require.Equal(t, want.ElapsedSeconds, got.ElapsedSeconds)
	require.Equal(t, want.HeartbeatAgeSeconds, got.HeartbeatAgeSeconds)
	require.Len(t, got.Labels, len(want.Labels))
	for i := range want.Labels {
		require.Equal(t, want.Labels[i].Key, got.Labels[i].Key)
		require.Equal(t, want.Labels[i].Value, got.Labels[i].Value)
		require.Equal(t, want.Labels[i].Source, got.Labels[i].Source)
		require.True(t, want.Labels[i].UpdatedAt.Equal(got.Labels[i].UpdatedAt))
	}
	require.Equal(t, want.LabelAgeSeconds, got.LabelAgeSeconds)
	require.Equal(t, want.Sheet, got.Sheet)
	require.True(t, want.SheetUpdatedAt.Equal(got.SheetUpdatedAt))
	require.Equal(t, want.SheetAgeSeconds, got.SheetAgeSeconds)
	require.Equal(t, want.SheetIsHostWide, got.SheetIsHostWide)
	require.Equal(t, want.RecentJobs, got.RecentJobs)
}

// TestDescribeShowsRecentJobsWithOutcomeAndDuration pins the job-history
// section: each recent job must show what happened to it and how long it
// ran, not just an ID nobody can act on.
func TestDescribeShowsRecentJobsWithOutcomeAndDuration(t *testing.T) {
	started := time.Now().Add(-90 * time.Second)
	finished := time.Now().Add(-30 * time.Second)
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		RecentJobs: []model.Job{
			{
				ID: "job-xyz", DeviceID: "gpubox:gpu0", State: model.JobFailed,
				Command: []string{"./train"}, StartedAt: &started, FinishedAt: &finished,
			},
		},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	require.Contains(t, out, "job-xyz")
	require.Contains(t, out, "failed")
	require.Contains(t, out, "1m0s", "the job's actual run duration must be shown")
}
