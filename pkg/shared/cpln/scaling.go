package cpln

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ScaleForOperation scales the workload to maxScale and returns a cleanup function
// that restores the original minScale. Call the cleanup function via defer.
// If the workload is already at full scale, returns a no-op cleanup.
func ScaleForOperation(client *Client, gvc, workloadName string) (maxScale int, cleanup func(), err error) {
	minScale, maxScale, err := client.GetWorkloadScaling(gvc, workloadName)
	if err != nil {
		return 0, func() {}, fmt.Errorf("failed to get workload scaling: %w", err)
	}

	if minScale >= maxScale {
		slog.Info("workload already at full scale, skipping scale-up", "minScale", minScale, "maxScale", maxScale)
		return maxScale, func() {}, nil
	}

	slog.Info("scaling up workload for operation", "workload", workloadName, "from", minScale, "to", maxScale)
	if err := client.PatchWorkloadMinScale(gvc, workloadName, maxScale); err != nil {
		return 0, func() {}, fmt.Errorf("failed to scale up: %w", err)
	}

	// Wait for all replicas to be ready (5 min timeout)
	if err := client.WaitForReplicasReady(gvc, workloadName, maxScale, 5*time.Minute); err != nil {
		slog.Warn("timed out waiting for replicas, proceeding anyway", "error", err, "maxScale", maxScale)
	} else {
		slog.Info("all replicas ready after scale-up", "count", maxScale)
	}

	// Cleanup function restores the original minScale (safe to call multiple times)
	var once sync.Once
	cleanup = func() {
		once.Do(func() {
			slog.Info("restoring minScale after operation", "workload", workloadName, "minScale", minScale)
			maxAttempts := 10
			retryInterval := 15 * time.Second
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				if err := client.PatchWorkloadMinScale(gvc, workloadName, minScale); err != nil {
					slog.Error("failed to restore minScale, retrying",
						"error", err, "attempt", attempt, "maxAttempts", maxAttempts)
					if attempt < maxAttempts {
						time.Sleep(retryInterval)
					}
				} else {
					slog.Info("minScale restored successfully", "workload", workloadName, "minScale", minScale)
					return
				}
			}
			slog.Error("CRITICAL: failed to restore minScale after all attempts — manual intervention required",
				"workload", workloadName, "minScale", minScale)
		})
	}

	return maxScale, cleanup, nil
}
