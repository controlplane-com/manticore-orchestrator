package actions

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/client"
	"github.com/controlplane-com/manticore-orchestrator/pkg/indexer"
	"github.com/google/uuid"
)

// Import performs a coordinated import across all replicas
// Uses blue-green pattern: import to inactive slot, swap, drop old
// Accepts a context for cancellation support (e.g., SIGTERM handling)
// Supports multi-segment distributed tables via ctx.SegmentCount and ctx.CSVPaths.
func Import(goCtx context.Context, ctx *Context) error {
	slog.Debug("starting coordinated import")

	if len(ctx.Clients) == 0 {
		return fmt.Errorf("no clients configured")
	}

	// Use replica-0 for cluster operations
	primary := ctx.Clients[0]

	// Fetch table config from agent to determine clustering, HA options, and segment count
	tableConfig, err := primary.GetTableConfig(ctx.Dataset, 1)
	if err != nil {
		slog.Warn("failed to fetch table config, using defaults", "table", ctx.Dataset, "error", err)
		tableConfig = &client.TableConfigResponse{
			ClusterMain:     true,
			HAStrategy:      "nodeads",
			AgentRetryCount: 0,
		}
	}
	slog.Debug("table config", "table", ctx.Dataset, "clusterMain", tableConfig.ClusterMain, "haStrategy", tableConfig.HAStrategy, "segmentCount", tableConfig.SegmentCount)

	// Apply segment count from agent config (overrides whatever was passed in ctx)
	ctx.SegmentCount = tableConfig.SegmentCount
	if ctx.SegmentCount < 1 {
		ctx.SegmentCount = 1
	}

	// Step 1: Discover current slot from agents
	currentSlot := DiscoverTableSlot(ctx.Clients, ctx.Dataset)
	slog.Debug("discovered current slot", "table", ctx.Dataset, "slot", currentSlot)

	// Determine new and old slots (blue-green) for this specific table
	var newSlot, oldSlot string
	if currentSlot == "a" {
		newSlot = "b"
		oldSlot = "a"
	} else {
		newSlot = "a"
		oldSlot = "b"
	}

	distTable := ctx.DistributedTableName()
	slog.Debug("import plan", "distTable", distTable, "newSlot", newSlot, "oldSlot", oldSlot, "segmentCount", ctx.SegmentCount, "clusterMain", tableConfig.ClusterMain)

	// cleanup drops all new segment main tables on all replicas
	cleanup := func() {
		slog.Info("cleaning up failed import", "dataset", ctx.Dataset, "newSlot", newSlot, "segmentCount", ctx.SegmentCount)

		for seg := 1; seg <= ctx.SegmentCount; seg++ {
			newMainTable := ctx.SegmentMainTableName(newSlot, seg)
			for i, c := range ctx.Clients {
				if err := c.DropTable(newMainTable, 0); err != nil {
					slog.Error("failed to drop table during cleanup", "table", newMainTable, "replica", i, "error", err)
				}
			}
		}
	}

	importConfig := client.ImportConfigFromEnv()

	// Loop over segments, build/upload/import each
	for seg := 1; seg <= ctx.SegmentCount; seg++ {
		newMainTable := ctx.SegmentMainTableName(newSlot, seg)
		segCSVPath := ctx.csvPathForSegment(seg)
		slog.Info("importing segment (indexer)", "seg", seg, "table", newMainTable, "csv", segCSVPath)
		if err := buildAndImportIndex(goCtx, ctx, segCSVPath, newMainTable, tableConfig, primary, cleanup); err != nil {
			return err
		}
		if tableConfig.ClusterMain {
			if err := waitForReplication(goCtx, ctx, newMainTable); err != nil {
				cleanup()
				return fmt.Errorf("replication wait failed for segment %d: %w", seg, err)
			}
		}
	}

	// Step 5b: Verify table exists on all replicas before proceeding to ALTER DISTRIBUTED.
	// After a replica restart, the table may not exist even if the import reported success
	// If a table is missing, re-run the import on that specific replica.
	for seg := 1; seg <= ctx.SegmentCount; seg++ {
		segMainTable := ctx.SegmentMainTableName(newSlot, seg)
		segCSVPath := ctx.csvPathForSegment(seg)
		slog.Debug("verifying table exists on all replicas before swap", "table", segMainTable)
		for i, c := range ctx.Clients {
			if err := ensureTableOnReplica(goCtx, c, i, segMainTable, segCSVPath, importConfig); err != nil {
				cleanup()
				return fmt.Errorf("table recovery failed before swap (seg %d): %w", seg, err)
			}
		}
	}

	// Step 6: Atomic swap - ALTER distributed table on ALL replicas with all segments
	allNewMains := ctx.AllMainTableNames(newSlot)
	allDeltas := ctx.AllDeltaTableNames()
	locals := append(allNewMains, allDeltas...)
	slog.Debug("swapping distributed table on all replicas", "table", distTable, "locals", locals)

	// Build agent list: ALL replicas (same config everywhere)
	var agents []string
	for _, c := range ctx.Clients {
		agentAddr := extractAgentAddr(c.BaseURL())
		if agentAddr != "" {
			agents = append(agents, agentAddr)
		}
	}

	for i, c := range ctx.Clients {
		err := executeWithRetries(goCtx, fmt.Sprintf("alter distributed on replica %d", i), func() error {
			return c.AlterDistributed(distTable, locals, agents, tableConfig.HAStrategy, tableConfig.AgentRetryCount, 1)
		})
		if err != nil {
			return fmt.Errorf("failed to alter distributed table on replica %d: %w", i, err)
		}
		slog.Debug("swapped distributed table", "replica", i, "agents", agents)
	}

	// Step 7: Remove old segment main tables from cluster (if clustered), then drop on ALL replicas
	for seg := 1; seg <= ctx.SegmentCount; seg++ {
		oldMainTable := ctx.SegmentMainTableName(oldSlot, seg)
		if tableConfig.ClusterMain {
			slog.Debug("removing old main table from cluster", "table", oldMainTable)
			err := executeWithRetries(goCtx, "cluster drop "+oldMainTable, func() error {
				return primary.ClusterDrop(oldMainTable, 1)
			})
			if err != nil {
				slog.Warn("failed to remove old table from cluster", "table", oldMainTable, "error", err)
			}
		}
		slog.Debug("dropping old main table on all replicas", "table", oldMainTable)
		for i, c := range ctx.Clients {
			err := executeWithRetries(goCtx, fmt.Sprintf("drop table %s on replica %d", oldMainTable, i), func() error {
				return c.DropTable(oldMainTable, 1)
			})
			if err != nil {
				slog.Warn("failed to drop old table", "table", oldMainTable, "replica", i, "error", err)
			} else {
				slog.Debug("dropped old table", "table", oldMainTable, "replica", i)
			}
		}
	}

	slog.Info("import completed successfully", "clustered", tableConfig.ClusterMain, "segments", ctx.SegmentCount)
	return nil
}

// sharedVolumeSyncDelay returns the delay to wait after writing index to shared volume.
// Configurable via SHARED_VOLUME_SYNC_DELAY env var (e.g. "0s" for tests).
func sharedVolumeSyncDelay() time.Duration {
	if delay := os.Getenv("SHARED_VOLUME_SYNC_DELAY"); delay != "" {
		if d, err := time.ParseDuration(delay); err == nil {
			return d
		}
	}
	return 10 * time.Second
}

// tableExistsOnClient returns true if the named table appears in the client's table list.
func tableExistsOnClient(c *client.AgentClient, tableName string, retries int) (bool, error) {
	tables, err := c.ListTables(retries)
	if err != nil {
		return false, err
	}
	for _, t := range tables {
		if t.Name == tableName {
			return true, nil
		}
	}
	return false, nil
}

// waitForReplication waits for a table to be present on all replicas (Galera replication).
func waitForReplication(goCtx context.Context, ctx *Context, table string) error {
	maxAttempts := 30
	pollInterval := 2 * time.Second
	if d := os.Getenv("REPLICATION_POLL_INTERVAL"); d != "" {
		if pd, err := time.ParseDuration(d); err == nil {
			pollInterval = pd
		}
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-goCtx.Done():
			return goCtx.Err()
		default:
		}

		allReady := true
		for i, c := range ctx.Clients {
			exists, err := tableExistsOnClient(c, table, 0)
			if err != nil {
				slog.Debug("failed to list tables on replica", "replica", i, "error", err)
				allReady = false
			} else if !exists {
				slog.Debug("table not yet replicated", "replica", i, "table", table)
				allReady = false
			}
		}

		if allReady {
			slog.Debug("table replicated to all replicas", "table", table, "replicas", len(ctx.Clients))
			return nil
		}

		if attempt < maxAttempts {
			slog.Debug("waiting for replication", "attempt", attempt, "maxAttempts", maxAttempts)
			select {
			case <-goCtx.Done():
				return goCtx.Err()
			case <-time.After(pollInterval):
			}
		}
	}

	return fmt.Errorf("replication timeout: table %s not replicated to all nodes after %d attempts", table, maxAttempts)
}

// ensureTableOnReplica verifies a table exists on a replica, re-importing if needed.
// If the table is missing (e.g., after a replica restart during scaling), it re-creates
// and re-imports the table on that specific replica before proceeding.
func ensureTableOnReplica(goCtx context.Context, c *client.AgentClient, replicaIdx int,
	table, csvPath string, importConfig client.ImportConfig) error {

	if err := verifyTableExists(goCtx, c, table, replicaIdx); err != nil {
		slog.Warn("table missing on replica, re-importing",
			"replica", replicaIdx, "table", table, "verifyErr", err)
		if createErr := c.CreateTable(table, 0); createErr != nil {
			slog.Debug("CreateTable before re-import (may already exist)", "error", createErr)
		}
		if importErr := c.RunImport(goCtx, table, csvPath, "", 0, importConfig); importErr != nil {
			return fmt.Errorf("re-import on replica %d failed: %w", replicaIdx, importErr)
		}
		if err := verifyTableExists(goCtx, c, table, replicaIdx); err != nil {
			return fmt.Errorf("table still missing after re-import on replica %d: %w", replicaIdx, err)
		}
		slog.Info("table recovered on replica", "replica", replicaIdx, "table", table)
	}
	return nil
}

// verifyTableExists checks that a table exists on a specific replica.
// First waits up to 10 minutes for the agent to become reachable (handles rolling restarts),
// then checks for the table with a shorter timeout.
func verifyTableExists(goCtx context.Context, c *client.AgentClient, table string, replicaIdx int) error {
	// Phase 1: Wait for the agent to be reachable (up to 10 minutes).
	// During a rolling restart the agent returns 503 or is unreachable entirely.
	// WaitForHealth uses /api/health which bypasses K8s readiness gates.
	healthTimeout := 10 * time.Minute
	slog.Debug("waiting for agent health before table verification", "replica", replicaIdx, "timeout", healthTimeout)
	if err := c.WaitForHealth(goCtx, healthTimeout); err != nil {
		return fmt.Errorf("replica %d did not become healthy within %v: %w", replicaIdx, healthTimeout, err)
	}

	// Phase 2: Agent is healthy — verify the table exists.
	// After a restart with clustered tables, the table should reappear via Galera SST.
	// Poll for up to 2 minutes to allow replication/SST to complete.
	maxAttempts := 12 // 12 attempts × 10s = 2 minutes
	pollInterval := 10 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-goCtx.Done():
			return goCtx.Err()
		default:
		}

		exists, err := tableExistsOnClient(c, table, 1)
		if err != nil {
			slog.Warn("table verification: ListTables failed on healthy agent",
				"replica", replicaIdx, "attempt", attempt, "error", err)
		} else if exists {
			slog.Debug("table verified on replica", "replica", replicaIdx, "table", table)
			return nil
		} else {
			slog.Warn("table not found on replica after import",
				"replica", replicaIdx, "table", table, "attempt", attempt)
		}

		if attempt < maxAttempts {
			select {
			case <-goCtx.Done():
				return goCtx.Err()
			case <-time.After(pollInterval):
			}
		}
	}

	return fmt.Errorf("table %s not found on replica %d after agent recovered (table may have been lost during restart)", table, replicaIdx)
}

// executeWithRetries retries an operation every 30s for up to 5 minutes.
// This handles the case where a replica is restarting (e.g., from a rollout triggered by scaling)
// and the operation fails because the agent is temporarily unavailable.
func executeWithRetries(ctx context.Context, operation string, fn func() error) error {
	maxAttempts := 20 // 20 attempts × 30s = 10 minutes
	retryInterval := 30 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			if attempt > 1 {
				slog.Info("operation succeeded after retry", "operation", operation, "attempt", attempt)
			}
			return nil
		}

		if attempt == maxAttempts {
			break
		}

		slog.Warn("operation failed, retrying after wait",
			"operation", operation, "attempt", attempt, "maxAttempts", maxAttempts, "error", lastErr)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}

	return fmt.Errorf("%s failed after %d attempts: %w", operation, maxAttempts, lastErr)
}

// buildAndImportIndex builds an RT index from the given CSV onto the shared volume and imports it on agents
func buildAndImportIndex(goCtx context.Context, ctx *Context, csvPath, targetTable string, tableConfig *client.TableConfigResponse, primary *client.AgentClient, cleanup func()) error {
	if ctx.IndexerBuilder == nil {
		return fmt.Errorf("indexer builder not configured (required for indexer method)")
	}
	if ctx.SharedVolumeMount == "" {
		return fmt.Errorf("shared volume mount not configured (required for indexer method)")
	}

	// Generate unique import ID for this operation
	importID := uuid.New().String()

	// Build directly to shared volume — same path is accessible on all agent pods
	// Path structure: {SharedVolumeMount}/indexer-output/{dataset}/{importID}
	workDir := filepath.Join(ctx.SharedVolumeMount, "indexer-output", ctx.Dataset, importID)
	agentIndexPath := filepath.Join(workDir, "data", targetTable)

	slog.Info("starting indexer import",
		"table", targetTable,
		"importID", importID,
		"workDir", workDir)

	// Use columns from schema registry (YAML config) — preserves user-declared order
	var columns []indexer.Column
	for _, col := range tableConfig.Columns {
		columns = append(columns, indexer.Column{
			Name: col.Name,
			Type: col.Type, // Already in indexer-compatible format (attr_timestamp, field, etc.)
		})
	}

	// Source CSV lives on the data mount (read-only); shared volume is for indexer output only
	sourcePath := filepath.Join(ctx.S3Mount, csvPath)

	// Set memory limit: table config > context > default
	memLimit := tableConfig.MemLimit
	if memLimit == "" {
		memLimit = ctx.ImportMemLimit
	}
	if memLimit == "" {
		memLimit = indexer.DefaultMemLimit
	}

	// Build index
	cfg := &indexer.Config{
		WorkDir:      workDir,
		TableName:    targetTable,
		SourcePath:   sourcePath,
		Columns:      columns,
		MemLimit:     memLimit,
		CharsetTable: tableConfig.CharsetTable,
	}

	// Use explicit hasHeader from config if set, otherwise auto-detect
	if tableConfig.HasHeader != nil {
		cfg.HasHeader = *tableConfig.HasHeader
	}

	result, err := ctx.IndexerBuilder.Build(goCtx, cfg)
	if err != nil {
		cleanup()
		return fmt.Errorf("index build failed: %w", err)
	}
	slog.Info("index built", "rows", result.RowCount, "path", result.IndexPath)

	// Wait for the shared volume filesystem to sync the new index files across pods before importing
	syncDelay := sharedVolumeSyncDelay()
	slog.Info("waiting for shared volume sync before agent import", "delay", syncDelay, "path", agentIndexPath)
	select {
	case <-goCtx.Done():
		cleanup()
		return goCtx.Err()
	case <-time.After(syncDelay):
	}

	importConfig := client.ImportConfigFromEnv()
	importConfig.PrebuiltIndexPath = agentIndexPath

	// For clustered tables: import on primary only (replicates automatically)
	// For non-clustered: import on all replicas in parallel
	if tableConfig.ClusterMain {
		slog.Debug("importing prebuilt index (clustered)", "table", targetTable, "path", agentIndexPath)
		if err := primary.RunImport(goCtx, targetTable, csvPath, ctx.Cluster, 0, importConfig); err != nil {
			cleanup()
			return fmt.Errorf("failed to import prebuilt index: %w", err)
		}
		slog.Info("prebuilt index import completed on primary")
	} else {
		slog.Debug("importing prebuilt index on all replicas (non-clustered)", "table", targetTable, "path", agentIndexPath)

		importCtx, cancelImports := context.WithCancel(goCtx)
		defer cancelImports()

		var wg sync.WaitGroup
		errCh := make(chan error, len(ctx.Clients))

		for i, c := range ctx.Clients {
			wg.Add(1)
			go func(replicaIdx int, replicaClient *client.AgentClient) {
				defer wg.Done()
				slog.Debug("importing prebuilt index on replica", "replica", replicaIdx, "table", targetTable)
				if err := replicaClient.RunImport(importCtx, targetTable, csvPath, "", 0, importConfig); err != nil {
					slog.Error("prebuilt index import failed on replica", "replica", replicaIdx, "error", err)
					errCh <- fmt.Errorf("failed to import on replica %d: %w", replicaIdx, err)
					cancelImports()
				} else {
					slog.Info("prebuilt index import completed on replica", "replica", replicaIdx)
				}
			}(i, c)
		}

		wg.Wait()
		close(errCh)

		var errors []error
		for err := range errCh {
			errors = append(errors, err)
		}

		if len(errors) > 0 {
			cleanup()
			return fmt.Errorf("%d replica(s) failed: %w", len(errors), errors[0])
		}
	}

	return nil
}
