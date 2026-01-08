package actions

import (
	"fmt"
	"log"
)

// Init initializes the table structure on all replicas
// This creates: delta table, initial main table, and distributed table
func Init(ctx *Context) error {
	log.Println("Initializing table structure...")

	if len(ctx.Clients) == 0 {
		return fmt.Errorf("no clients configured")
	}

	// Use replica-0 for cluster operations (will replicate to others)
	primary := ctx.Clients[0]

	// Load current state to determine which slot to use
	state, err := ctx.LoadState()
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}

	mainTable := ctx.MainTableName(state.ActiveSlot)
	deltaTable := ctx.DeltaTableName()
	distTable := ctx.DistributedTableName()

	log.Printf("Creating tables: main=%s, delta=%s, distributed=%s", mainTable, deltaTable, distTable)

	// Step 1: Create delta table on replica-0
	log.Printf("Creating delta table: %s", deltaTable)
	if err := primary.CreateTable(deltaTable); err != nil {
		return fmt.Errorf("failed to create delta table: %w", err)
	}

	// Step 2: Add delta table to cluster (replicates to all nodes)
	log.Printf("Adding delta table to cluster: %s", deltaTable)
	if err := primary.ClusterAdd(deltaTable); err != nil {
		return fmt.Errorf("failed to add delta table to cluster: %w", err)
	}

	// Step 3: Create initial main table on replica-0
	log.Printf("Creating main table: %s", mainTable)
	if err := primary.CreateTable(mainTable); err != nil {
		return fmt.Errorf("failed to create main table: %w", err)
	}

	// Step 4: Add main table to cluster
	log.Printf("Adding main table to cluster: %s", mainTable)
	if err := primary.ClusterAdd(mainTable); err != nil {
		return fmt.Errorf("failed to add main table to cluster: %w", err)
	}

	// Step 5: Create distributed table on ALL replicas
	// Distributed tables are local metadata, not replicated
	locals := []string{mainTable, deltaTable}
	log.Printf("Creating distributed table on all replicas: %s -> %v", distTable, locals)

	for i, c := range ctx.Clients {
		if err := c.CreateDistributed(distTable, locals); err != nil {
			return fmt.Errorf("failed to create distributed table on replica %d: %w", i, err)
		}
		log.Printf("Created distributed table on replica %d", i)
	}

	log.Println("Table structure initialized successfully")
	return nil
}
