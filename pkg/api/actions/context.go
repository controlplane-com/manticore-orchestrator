package actions

import (
	"log/slog"

	"github.com/controlplane-com/manticore-orchestrator/api/client"
)

// Context holds the execution context for actions
type Context struct {
	Clients []*client.AgentClient
	Dataset string // e.g., "addresses"
	CSVPath string // e.g., "addresses.csv"
}

// MainTableName returns the main table name for a given slot
func (c *Context) MainTableName(slot string) string {
	return c.Dataset + "_main_" + slot
}

// DeltaTableName returns the delta table name
func (c *Context) DeltaTableName() string {
	return c.Dataset + "_delta"
}

// DistributedTableName returns the distributed table name
func (c *Context) DistributedTableName() string {
	return c.Dataset
}

// DiscoverTableSlots queries healthy cluster members for existing tables to determine which slot is active per table
// The agent returns the discovered slot in the ListTables response for each base table
// Returns empty string for tables where slot cannot be determined from distributed table
func DiscoverTableSlots(clients []*client.AgentClient, tableNames []string) map[string]string {
	slots := make(map[string]string)

	// Get tables from a healthy cluster member
	for i, c := range clients {
		// Check if this replica is healthy and part of the cluster
		health, err := c.Health(1) // Single attempt
		if err != nil {
			slog.Debug("skipping replica for slot discovery - health check failed", "replica", i, "error", err)
			continue
		}

		// Only consider replicas that are part of a cluster (primary or non-primary)
		if health.ClusterStatus != "primary" && health.ClusterStatus != "non-primary" {
			slog.Debug("skipping replica for slot discovery - not in cluster",
				"replica", i, "clusterStatus", health.ClusterStatus)
			continue
		}

		// Only consider replicas that are fully synced
		if health.NodeState != "synced" {
			slog.Debug("skipping replica for slot discovery - not synced",
				"replica", i, "nodeState", health.NodeState)
			continue
		}

		tables, err := c.ListTables(0)
		if err != nil {
			slog.Debug("skipping replica for slot discovery - list tables failed", "replica", i, "error", err)
			continue
		}

		// Use slot from agent's response for each configured table
		// Empty slot means agent couldn't determine from distributed table
		for _, tableName := range tableNames {
			for _, t := range tables {
				if t.Name == tableName {
					slots[tableName] = t.Slot // May be empty - that's intentional
					break
				}
			}
		}

		slog.Debug("discovered slots from healthy cluster member",
			"replica", i, "clusterStatus", health.ClusterStatus, "slots", slots)
		break // Got info from one healthy member
	}

	return slots
}

// DiscoverTableSlot returns the active slot for a single table
// Returns empty string if slot cannot be determined from distributed table
func DiscoverTableSlot(clients []*client.AgentClient, tableName string) string {
	slots := DiscoverTableSlots(clients, []string{tableName})
	return slots[tableName] // May be empty
}
