package actions

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/client"
	"github.com/controlplane-com/manticore-orchestrator/pkg/indexer"
)

// IndexBuilder is the interface for building Manticore RT indexes from source data
type IndexBuilder interface {
	Build(ctx context.Context, cfg *indexer.Config) (*indexer.BuildResult, error)
}

// Context holds the execution context for actions
type Context struct {
	Clients []*client.AgentClient
	Dataset string // e.g., "addresses"
	CSVPath string // e.g., "addresses.csv" — first (or only) CSV path, kept for compat
	Cluster string // Cluster name for replication

	// Multi-segment support
	CSVPaths     []string // one CSV path per segment; CSVPath = CSVPaths[0] when set
	SegmentCount int      // number of segments (1 = single-segment, current behavior)

	// Indexer-related configuration
	IndexerBuilder    IndexBuilder // Indexer for building RT indexes from source CSVs
	S3Mount           string      // Mount path for source CSVs (e.g., "/mnt/s3" or "/mnt/data")
	ImportMemLimit    string      // Memory limit for indexer (e.g., "2G")
	SharedVolumeMount string      // Shared volume mount path — same path on cron and all agents; indexer output written here
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

// SegmentMainTableName returns the main table name for a given slot and segment number.
// When SegmentCount <= 1 (single-segment), returns the current naming (no suffix).
func (c *Context) SegmentMainTableName(slot string, seg int) string {
	if c.SegmentCount <= 1 {
		return c.Dataset + "_main_" + slot
	}
	return fmt.Sprintf("%s_main_%s_%d", c.Dataset, slot, seg)
}

// SegmentDeltaTableName returns the delta table name.
// Delta tables are never segmented — all real-time writes go to a single delta table
// regardless of segmentCount. The seg parameter is kept for API compatibility.
func (c *Context) SegmentDeltaTableName(seg int) string {
	return c.Dataset + "_delta"
}

// AllMainTableNames returns all segment main table names for a given slot.
func (c *Context) AllMainTableNames(slot string) []string {
	count := c.SegmentCount
	if count < 1 {
		count = 1
	}
	names := make([]string, count)
	for i := range names {
		names[i] = c.SegmentMainTableName(slot, i+1)
	}
	return names
}

// AllDeltaTableNames returns the delta table name as a single-element slice.
// Delta tables are never segmented regardless of segmentCount.
func (c *Context) AllDeltaTableNames() []string {
	return []string{c.DeltaTableName()}
}

// csvPathForSegment returns the CSV path for a given segment number (1-based).
func (c *Context) csvPathForSegment(seg int) string {
	if len(c.CSVPaths) >= seg {
		return c.CSVPaths[seg-1]
	}
	return c.CSVPath // fallback for single-segment or missing paths
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
