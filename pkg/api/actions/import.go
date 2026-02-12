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
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
	"github.com/google/uuid"
)

// Import performs a coordinated import across all replicas
// Uses blue-green pattern: import to inactive slot, swap, drop old
// Accepts a context for cancellation support (e.g., SIGTERM handling)
func Import(goCtx context.Context, ctx *Context) error {
	slog.Debug("starting coordinated import")

	if len(ctx.Clients) == 0 {
		return fmt.Errorf("no clients configured")
	}

	// Use replica-0 for cluster operations
	primary := ctx.Clients[0]

	// Fetch table config from agent to determine import method, clustering, and HA options
	tableConfig, err := primary.GetTableConfig(ctx.Dataset, 1)
	if err != nil {
		slog.Warn("failed to fetch table config, using defaults", "table", ctx.Dataset, "error", err)
		tableConfig = &client.TableConfigResponse{
			ImportMethod:    "bulk",
			ClusterMain:     true,
			HAStrategy:      "nodeads",
			AgentRetryCount: 0,
		}
	}
	slog.Debug("table config", "table", ctx.Dataset, "importMethod", tableConfig.ImportMethod, "clusterMain", tableConfig.ClusterMain, "haStrategy", tableConfig.HAStrategy)

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

	newMainTable := ctx.MainTableName(newSlot)
	oldMainTable := ctx.MainTableName(oldSlot)
	deltaTable := ctx.DeltaTableName()
	distTable := ctx.DistributedTableName()

	slog.Debug("import plan", "newTable", newMainTable, "oldTable", oldMainTable, "deltaTable", deltaTable, "clusterMain", tableConfig.ClusterMain)

	// Track if ClusterAdd succeeded - cleanup is needed on failure after this point
	var clusterAddSucceeded bool

	// cleanup removes the new table from cluster and drops it on all replicas
	// Called when import fails after ClusterAdd to prevent orphaned tables
	cleanup := func() {
		slog.Info("cleaning up failed import", "table", newMainTable)

		// Only remove from cluster if we added it
		if clusterAddSucceeded && tableConfig.ClusterMain {
			if err := primary.ClusterDrop(newMainTable, 0); err != nil {
				slog.Error("failed to remove table from cluster during cleanup", "table", newMainTable, "error", err)
			}
		}

		// Drop on all replicas
		for i, c := range ctx.Clients {
			if err := c.DropTable(newMainTable, 0); err != nil {
				slog.Error("failed to drop table during cleanup", "table", newMainTable, "replica", i, "error", err)
			}
		}
	}

	// Step 2: Create new main table on replica-0
	// Skip table creation for indexer method - IMPORT TABLE creates the table from index files
	if tableConfig.ImportMethod != "indexer" {
		slog.Debug("creating new main table", "table", newMainTable)
		if err := primary.CreateTable(newMainTable, 0); err != nil {
			return fmt.Errorf("failed to create new main table: %w", err)
		}

		// Step 3: Conditionally add new table to cluster based on schema config
		if tableConfig.ClusterMain {
			slog.Debug("adding new main table to cluster", "table", newMainTable)
			if err := primary.ClusterAdd(newMainTable, 0); err != nil {
				return fmt.Errorf("failed to add new main table to cluster: %w", err)
			}
			clusterAddSucceeded = true
		} else {
			slog.Debug("skipping cluster add for main table (clusterMain=false)", "table", newMainTable)
			// When not clustered, we need to create the table on all replicas
			for i, c := range ctx.Clients[1:] {
				slog.Debug("creating main table on replica", "replica", i+1, "table", newMainTable)
				if err := c.CreateTable(newMainTable, 0); err != nil {
					cleanup()
					return fmt.Errorf("failed to create main table on replica %d: %w", i+1, err)
				}
			}
		}
	} else {
		slog.Debug("skipping table creation for indexer method (IMPORT TABLE will create it)", "table", newMainTable)
	}

	// Step 4: Run import with schema-specified method
	importConfig := client.ImportConfigFromEnv()
	importConfig.Method = types.ImportMethod(tableConfig.ImportMethod)

	if tableConfig.ImportMethod == "indexer" {
		// Indexer method: build locally, upload to S3, then import on agents
		if err := importWithIndexer(goCtx, ctx, newMainTable, tableConfig, primary, cleanup); err != nil {
			return err
		}

		// Wait for replication if clustered
		if tableConfig.ClusterMain {
			slog.Debug("waiting for replication")
			if err := waitForReplication(goCtx, ctx, newMainTable); err != nil {
				cleanup()
				return fmt.Errorf("replication wait failed: %w", err)
			}
		}
	} else if tableConfig.ClusterMain {
		// Clustered bulk import: import on primary, data replicates automatically
		slog.Debug("importing CSV to table (clustered)", "table", newMainTable, "csv", ctx.CSVPath, "method", tableConfig.ImportMethod)
		if err := primary.ImportWithContext(goCtx, newMainTable, ctx.CSVPath, ctx.Cluster, 0, importConfig); err != nil {
			cleanup()
			return fmt.Errorf("failed to import CSV: %w", err)
		}
		slog.Debug("import completed on primary, waiting for replication")

		// Step 5: Wait for replication to complete
		if err := waitForReplication(goCtx, ctx, newMainTable); err != nil {
			cleanup()
			return fmt.Errorf("replication wait failed: %w", err)
		}
	} else {
		// Non-clustered bulk import: import on ALL replicas simultaneously
		slog.Debug("importing CSV to table on all replicas (non-clustered)", "table", newMainTable, "csv", ctx.CSVPath, "method", tableConfig.ImportMethod)

		// Create cancellable context so we can abort remaining imports on first failure
		importCtx, cancelImports := context.WithCancel(goCtx)
		defer cancelImports()

		var wg sync.WaitGroup
		errCh := make(chan error, len(ctx.Clients))

		for i, c := range ctx.Clients {
			wg.Add(1)
			go func(replicaIdx int, replicaClient *client.AgentClient) {
				defer wg.Done()
				slog.Debug("importing on replica", "replica", replicaIdx, "table", newMainTable)
				if err := replicaClient.ImportWithContext(importCtx, newMainTable, ctx.CSVPath, "", 0, importConfig); err != nil {
					slog.Error("import failed on replica", "replica", replicaIdx, "table", newMainTable, "error", err)
					errCh <- fmt.Errorf("failed to import CSV on replica %d: %w", replicaIdx, err)
					cancelImports() // Signal other imports to abort
				} else {
					slog.Info("import completed on replica", "replica", replicaIdx, "table", newMainTable)
				}
			}(i, c)
		}

		wg.Wait()
		close(errCh)

		// Collect all errors that occurred
		var errors []error
		for err := range errCh {
			errors = append(errors, err)
		}

		if len(errors) > 0 {
			cleanup()
			return fmt.Errorf("%d replica(s) failed: %w", len(errors), errors[0])
		}
	}

	// Step 6: Atomic swap - ALTER distributed table on ALL replicas
	// This is the critical step - all replicas must point to new table before dropping old
	locals := []string{newMainTable, deltaTable}
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
		if err := c.AlterDistributed(distTable, locals, agents, tableConfig.HAStrategy, tableConfig.AgentRetryCount, 0); err != nil {
			return fmt.Errorf("failed to alter distributed table on replica %d: %w", i, err)
		}
		slog.Debug("swapped distributed table", "replica", i, "agents", agents)
	}

	// Step 7: Remove old table from cluster (if it was clustered), then drop on ALL replicas
	if tableConfig.ClusterMain {
		slog.Debug("removing old main table from cluster", "table", oldMainTable)
		if err := primary.ClusterDrop(oldMainTable, 0); err != nil {
			slog.Warn("failed to remove old table from cluster", "table", oldMainTable, "error", err)
		}
	}

	// Drop the table on each replica
	slog.Debug("dropping old main table on all replicas", "table", oldMainTable)
	for i, c := range ctx.Clients {
		if err := c.DropTable(oldMainTable, 0); err != nil {
			slog.Warn("failed to drop old table", "table", oldMainTable, "replica", i, "error", err)
		} else {
			slog.Debug("dropped old table", "table", oldMainTable, "replica", i)
		}
	}

	slog.Info("import completed successfully", "method", tableConfig.ImportMethod, "clustered", tableConfig.ClusterMain)
	return nil
}

// waitForReplication waits for a table to be replicated to all nodes
func waitForReplication(goCtx context.Context, ctx *Context, table string) error {
	maxAttempts := 30
	pollInterval := 2 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check for context cancellation
		select {
		case <-goCtx.Done():
			return goCtx.Err()
		default:
		}

		allReady := true

		for i, c := range ctx.Clients {
			tables, err := c.ListTables(0)
			if err != nil {
				slog.Debug("failed to list tables", "replica", i, "error", err)
				allReady = false
				continue
			}

			found := false
			for _, t := range tables {
				if t.Name == table {
					found = true
					break
				}
			}

			if !found {
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

// importWithIndexer builds index locally, uploads to S3 or shared volume, and imports on agents
func importWithIndexer(goCtx context.Context, ctx *Context, targetTable string, tableConfig *client.TableConfigResponse, primary *client.AgentClient, cleanup func()) error {
	// Validate indexer builder is configured
	if ctx.IndexerBuilder == nil {
		return fmt.Errorf("indexer builder not configured (required for indexer method)")
	}

	// Determine storage mode: shared volume or S3
	useSharedVolume := ctx.SharedVolumeMount != ""
	if !useSharedVolume && ctx.S3Client == nil {
		return fmt.Errorf("neither S3 client nor shared volume configured (one is required for indexer method)")
	}

	// Generate unique import ID for this operation
	importID := uuid.New().String()

	// Determine work directory and agent path based on storage mode
	var workDir, agentIndexPath string
	if useSharedVolume {
		// Shared volume mode: build directly on shared volume
		// Path structure: {SharedVolumeMount}/indexer-output/{dataset}/{importID}
		workDir = filepath.Join(ctx.SharedVolumeMount, "indexer-output", ctx.Dataset, importID)
		agentIndexPath = filepath.Join(workDir, "data", targetTable) // RT index is in data/{tableName}
		slog.Info("using shared volume for indexer",
			"workDir", workDir,
			"agentPath", agentIndexPath)
	} else {
		// S3 mode: build locally, then upload
		workDir = filepath.Join(ctx.IndexerWorkDir, importID)
	}
	s3Path := fmt.Sprintf("%s/%s/%s", ctx.S3IndexPrefix, ctx.Dataset, importID)

	// Cleanup work directory when done (only for S3 mode - shared volume cleanup happens after all agents import)
	if !useSharedVolume {
		defer func() {
			if err := os.RemoveAll(workDir); err != nil {
				slog.Warn("failed to cleanup work dir", "path", workDir, "error", err)
			}
		}()
	}

	slog.Info("starting indexer import",
		"table", targetTable,
		"importID", importID,
		"workDir", workDir,
		"useSharedVolume", useSharedVolume)

	// Use columns from schema registry (YAML config) — preserves user-declared order
	var columns []indexer.Column
	for _, col := range tableConfig.Columns {
		columns = append(columns, indexer.Column{
			Name: col.Name,
			Type: col.Type, // Already in indexer-compatible format (attr_timestamp, field, etc.)
		})
	}

	// Determine source path - CSV is always on S3 mount (read-only)
	// The shared volume is only used for indexer output, not for source data
	sourcePath := filepath.Join(ctx.S3Mount, ctx.CSVPath)

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
		WorkDir:    workDir,
		TableName:  targetTable,
		SourcePath: sourcePath,
		Columns:    columns,
		MemLimit:   memLimit,
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

	// For S3 mode: upload to S3 and construct agent path
	if !useSharedVolume {
		if err := ctx.S3Client.UploadDirectory(goCtx, result.IndexPath, s3Path); err != nil {
			cleanup()
			return fmt.Errorf("S3 upload failed: %w", err)
		}
		slog.Info("index uploaded to S3", "path", s3Path)

		// Agents have S3 mounted at ctx.S3Mount, so they access: {S3Mount}/{s3Path}
		agentIndexPath = filepath.Join(ctx.S3Mount, s3Path)
	}
	// For shared volume mode: agentIndexPath is already set (same path as where we built)
	// Add a delay to allow shared volume filesystem to sync across pods
	if useSharedVolume {
		syncDelay := 10 * time.Second
		slog.Info("waiting for shared volume sync before agent import", "delay", syncDelay, "path", agentIndexPath)
		select {
		case <-goCtx.Done():
			cleanup()
			return goCtx.Err()
		case <-time.After(syncDelay):
		}
	}

	// Build import config with prebuilt path
	importConfig := client.ImportConfigFromEnv()
	importConfig.Method = types.ImportMethodIndexer
	importConfig.PrebuiltIndexPath = agentIndexPath

	// For clustered tables: import on primary only (replicates automatically)
	// For non-clustered: import on all replicas in parallel
	if tableConfig.ClusterMain {
		slog.Debug("importing prebuilt index (clustered)", "table", targetTable, "path", agentIndexPath)
		if err := primary.ImportWithContext(goCtx, targetTable, ctx.CSVPath, ctx.Cluster, 0, importConfig); err != nil {
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
				if err := replicaClient.ImportWithContext(importCtx, targetTable, ctx.CSVPath, "", 0, importConfig); err != nil {
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

