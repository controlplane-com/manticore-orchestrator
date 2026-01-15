package actions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/api/client"
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

	slog.Debug("import plan", "newTable", newMainTable, "oldTable", oldMainTable, "deltaTable", deltaTable)

	// Track if ClusterAdd succeeded - cleanup is needed on failure after this point
	var clusterAddSucceeded bool

	// cleanup removes the new table from cluster and drops it on all replicas
	// Called when import fails after ClusterAdd to prevent orphaned tables
	cleanup := func() {
		if !clusterAddSucceeded {
			return
		}
		slog.Info("cleaning up failed import", "table", newMainTable)

		// Remove from cluster first (required before DROP TABLE)
		if err := primary.ClusterDrop(newMainTable, 0); err != nil {
			slog.Error("failed to remove table from cluster during cleanup", "table", newMainTable, "error", err)
		}

		// Drop on all replicas
		for i, c := range ctx.Clients {
			if err := c.DropTable(newMainTable, 0); err != nil {
				slog.Error("failed to drop table during cleanup", "table", newMainTable, "replica", i, "error", err)
			}
		}
	}

	// Step 2: Create new main table on replica-0
	slog.Debug("creating new main table", "table", newMainTable)
	if err := primary.CreateTable(newMainTable, 0); err != nil {
		return fmt.Errorf("failed to create new main table: %w", err)
	}

	// Step 3: Add new table to cluster (replicates structure to all nodes)
	slog.Debug("adding new main table to cluster", "table", newMainTable)
	if err := primary.ClusterAdd(newMainTable, 0); err != nil {
		return fmt.Errorf("failed to add new main table to cluster: %w", err)
	}
	clusterAddSucceeded = true // Cleanup is now needed on any subsequent failure

	// Step 4: Run import on replica-0 (data replicates via cluster)
	// Use context-aware import to support cancellation on SIGTERM
	slog.Debug("importing CSV to table", "table", newMainTable, "csv", ctx.CSVPath)
	if err := primary.ImportWithContext(goCtx, newMainTable, ctx.CSVPath, 0, client.ImportConfigFromEnv()); err != nil {
		cleanup()
		return fmt.Errorf("failed to import CSV: %w", err)
	}
	slog.Debug("import completed on primary, waiting for replication")

	// Step 5: Wait for replication to complete
	// Give the cluster time to replicate the data
	if err := waitForReplication(goCtx, ctx, newMainTable); err != nil {
		cleanup()
		return fmt.Errorf("replication wait failed: %w", err)
	}

	// Step 6: Atomic swap - ALTER distributed table on ALL replicas
	// This is the critical step - all replicas must point to new table before dropping old
	locals := []string{newMainTable, deltaTable}
	slog.Debug("swapping distributed table on all replicas", "table", distTable, "locals", locals)

	for i, c := range ctx.Clients {
		if err := c.AlterDistributed(distTable, locals, 0); err != nil {
			return fmt.Errorf("failed to alter distributed table on replica %d: %w", i, err)
		}
		slog.Debug("swapped distributed table", "replica", i)
	}

	// Step 7: Remove old table from cluster, then drop on ALL replicas
	// ClusterDrop removes from cluster but table still exists locally on each node
	slog.Debug("removing old main table from cluster", "table", oldMainTable)
	if err := primary.ClusterDrop(oldMainTable, 0); err != nil {
		slog.Warn("failed to remove old table from cluster", "table", oldMainTable, "error", err)
	}

	// Drop the table on each replica (no longer cluster-managed)
	slog.Debug("dropping old main table on all replicas", "table", oldMainTable)
	for i, c := range ctx.Clients {
		if err := c.DropTable(oldMainTable, 0); err != nil {
			slog.Warn("failed to drop old table", "table", oldMainTable, "replica", i, "error", err)
		} else {
			slog.Debug("dropped old table", "table", oldMainTable, "replica", i)
		}
	}

	slog.Info("import completed successfully")
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
