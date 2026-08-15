package worker

import "context"

// GatherLabelsForTest is a thin exported wrapper over the unexported
// gatherLabels, so the external worker_test package can exercise it without
// widening the production API.
func (w *Worker) GatherLabelsForTest(ctx context.Context) ProbeResult {
	return w.gatherLabels(ctx)
}

// RunVerifyForTest is a thin exported wrapper over the unexported runVerify,
// so the external worker_test package can exercise it without widening the
// production API.
func (w *Worker) RunVerifyForTest(ctx context.Context, deviceID, jobID string) VerifyResult {
	return w.runVerify(ctx, deviceID, jobID)
}
