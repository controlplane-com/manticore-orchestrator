package actions

import (
	"fmt"
	"log"
	"time"
)

// Import performs a coordinated import across all replicas
// Uses blue-green pattern: import to inactive slot, swap, drop old
func Import(ctx *Context) error {
	log.Println("Starting coordinated import...")

	if len(ctx.Clients) == 0 {
		return fmt.Errorf("no clients configured")
	}

	// Use replica-0 for cluster operations
	primary := ctx.Clients[0]

	// Step 1: Load current state to determine slots
	state, err := ctx.LoadState()
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}

	// Determine new and old slots (blue-green)
	var newSlot, oldSlot string
	if state.ActiveSlot == "a" {
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

	log.Printf("Import plan: new=%s, old=%s, delta=%s", newMainTable, oldMainTable, deltaTable)

	// Step 2: Create new main table on replica-0
	log.Printf("Creating new main table: %s", newMainTable)
	if err := primary.CreateTable(newMainTable); err != nil {
		return fmt.Errorf("failed to create new main table: %w", err)
	}

	// Step 3: Add new table to cluster (replicates structure to all nodes)
	log.Printf("Adding new main table to cluster: %s", newMainTable)
	if err := primary.ClusterAdd(newMainTable); err != nil {
		return fmt.Errorf("failed to add new main table to cluster: %w", err)
	}

	// Step 4: Run import on replica-0 (data replicates via cluster)
	log.Printf("Importing CSV to table: %s from %s", newMainTable, ctx.CSVPath)
	if err := primary.Import(newMainTable, ctx.CSVPath); err != nil {
		return fmt.Errorf("failed to import CSV: %w", err)
	}
	log.Println("Import completed on primary, waiting for replication...")

	// Step 5: Wait for replication to complete
	// Give the cluster time to replicate the data
	if err := waitForReplication(ctx, newMainTable); err != nil {
		return fmt.Errorf("replication wait failed: %w", err)
	}

	// Step 6: Atomic swap - ALTER distributed table on ALL replicas
	// This is the critical step - all replicas must point to new table before dropping old
	locals := []string{newMainTable, deltaTable}
	log.Printf("Swapping distributed table on all replicas: %s -> %v", distTable, locals)

	for i, c := range ctx.Clients {
		if err := c.AlterDistributed(distTable, locals); err != nil {
			return fmt.Errorf("failed to alter distributed table on replica %d: %w", i, err)
		}
		log.Printf("Swapped distributed table on replica %d", i)
	}

	// Step 7: Update state to new slot BEFORE dropping old table
	state.ActiveSlot = newSlot
	if err := ctx.SaveState(state); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}
	log.Printf("Updated active slot to: %s", newSlot)

	// Step 8: Drop old main table (cluster-wide operation)
	log.Printf("Dropping old main table: %s", oldMainTable)
	if err := primary.DropTable(oldMainTable); err != nil {
		// Log but don't fail - the swap was successful
		log.Printf("Warning: failed to drop old table %s: %v", oldMainTable, err)
	}

	log.Println("Import completed successfully")
	return nil
}

// waitForReplication waits for a table to be replicated to all nodes
func waitForReplication(ctx *Context, table string) error {
	maxAttempts := 30
	pollInterval := 2 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		allReady := true

		for i, c := range ctx.Clients {
			tables, err := c.ListTables()
			if err != nil {
				log.Printf("Replica %d: failed to list tables: %v", i, err)
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
				log.Printf("Replica %d: table %s not yet replicated", i, table)
				allReady = false
			}
		}

		if allReady {
			log.Printf("Table %s replicated to all %d replicas", table, len(ctx.Clients))
			return nil
		}

		if attempt < maxAttempts {
			log.Printf("Waiting for replication (attempt %d/%d)...", attempt, maxAttempts)
			time.Sleep(pollInterval)
		}
	}

	return fmt.Errorf("replication timeout: table %s not replicated to all nodes after %d attempts", table, maxAttempts)
}
