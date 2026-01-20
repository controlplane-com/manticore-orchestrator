package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/controlplane-com/manticore-orchestrator/pkg/agent/manticore"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
)

// RunIndexerImport imports a pre-built index from the given path
// The index must have been built by the API and uploaded to S3
func (h *Handler) RunIndexerImport(ctx context.Context, job *types.ImportJob, schema *manticore.Schema) error {
	if job.PrebuiltIndexPath == "" {
		return fmt.Errorf("prebuiltIndexPath is required for indexer method")
	}

	// Verify path exists on S3 mount
	if _, err := os.Stat(job.PrebuiltIndexPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("prebuilt index not found at %s", job.PrebuiltIndexPath)
		}
		return fmt.Errorf("failed to stat prebuilt index at %s: %w", job.PrebuiltIndexPath, err)
	}

	// Execute IMPORT TABLE
	importSQL := fmt.Sprintf("IMPORT TABLE %s FROM '%s'", job.Table, job.PrebuiltIndexPath)
	slog.Info("importing prebuilt index", "table", job.Table, "path", job.PrebuiltIndexPath)

	if err := h.client.Execute(importSQL); err != nil {
		return fmt.Errorf("IMPORT TABLE failed: %w", err)
	}

	slog.Info("prebuilt index imported successfully", "table", job.Table)
	return nil
}

// executeIndexerImport runs the indexer import and updates job status
func (h *Handler) executeIndexerImport(ctx context.Context, job *types.ImportJob, csvFullPath string, tableSchema *manticore.Schema) {
	slog.Info("starting indexer import job",
		"jobId", job.ID,
		"table", job.Table,
		"prebuiltIndexPath", job.PrebuiltIndexPath)

	// Run the indexer import
	err := h.RunIndexerImport(ctx, job, tableSchema)

	// Check if cancelled
	if err == context.Canceled {
		h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusCancelled, "")
		slog.Info("indexer import job was cancelled", "jobId", job.ID)
		return
	}

	// Check for errors
	if err != nil {
		h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusFailed,
			fmt.Sprintf("indexer import failed: %v", err))
		slog.Error("indexer import job failed", "jobId", job.ID, "error", err)
		return
	}

	// Update final stats and mark completed
	h.jobManager.UpdateJob(job)
	h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusCompleted, "")
	slog.Info("indexer import job completed successfully",
		"jobId", job.ID,
		"processedLines", job.ProcessedLines)
}
