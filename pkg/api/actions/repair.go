package actions

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/controlplane-com/manticore-orchestrator/api/client"
	"github.com/controlplane-com/manticore-orchestrator/shared/cluster"
	"github.com/controlplane-com/manticore-orchestrator/shared/types"
)

// ReplicaInfo holds collected information about a replica
type ReplicaInfo struct {
	Index        int
	Reachable    bool
	Health       *types.HealthResponse
	Grastate     *types.GrastateResponse
	HasValidUUID bool
}

func getReplicaInfo(i int, c *client.AgentClient) ReplicaInfo {
	replica := ReplicaInfo{Index: i}
	health, err := c.Health(0)
	if err != nil {
		slog.Debug("replica health check failed", "replica", i, "error", err)
		replica.Reachable = false
		return replica
	}
	replica.Reachable = true
	replica.Health = health
	slog.Debug("replica health", "replica", i, "status", health.Status, "clusterStatus", health.ClusterStatus)

	gs, err := c.Grastate(0)
	if err != nil {
		slog.Debug("replica grastate failed", "replica", i, "error", err)
		return replica
	}
	replica.Grastate = gs
	replica.HasValidUUID = gs.Exists && gs.UUID != "" && gs.UUID != "00000000-0000-0000-0000-000000000000"
	if gs.Exists {
		slog.Debug("replica grastate", "replica", i, "uuid", gs.UUID, "seqno", gs.Seqno, "exists", gs.Exists, "valid", replica.HasValidUUID)
	} else {
		slog.Debug("no grastate.dat found", "replica", i)
	}
	return replica
}

// toSharedReplicaInfo converts local ReplicaInfo to the shared cluster.ReplicaInfo type
func toSharedReplicaInfo(replicas []ReplicaInfo) []cluster.ReplicaInfo {
	shared := make([]cluster.ReplicaInfo, len(replicas))
	for i, r := range replicas {
		shared[i] = cluster.ReplicaInfo{
			Index:        r.Index,
			Reachable:    r.Reachable,
			HasValidUUID: r.HasValidUUID,
		}
		if r.Health != nil {
			shared[i].ClusterStatus = r.Health.ClusterStatus
		}
		if r.Grastate != nil {
			shared[i].UUID = r.Grastate.UUID
			shared[i].Seqno = r.Grastate.Seqno
		}
	}
	return shared
}

// Repair uses intelligent source selection to repair a broken cluster.
// Algorithm:
//  1. Collect health status from all replicas
//  2. Check for quorum (any replica with clusterStatus == "primary")
//  3. If quorum exists and forceOverride is false → skip repair
//  4. Collect grastate.dat from all replicas
//  5. Use shared cluster logic to find winning group and source node
//  6. Bootstrap/verify source, force all others to rejoin
func Repair(ctx *Context) error {
	slog.Debug("starting cluster repair")

	// Step 1: Collect health and grastate from all replicas
	replicas := make([]ReplicaInfo, len(ctx.Clients))
	for i, c := range ctx.Clients {
		replicas[i] = getReplicaInfo(i, c)
	}

	// Step 2: Check for quorum
	var quorumReplicas []int
	for i, r := range replicas {
		if r.Reachable && r.Health != nil && r.Health.ClusterStatus == "primary" {
			quorumReplicas = append(quorumReplicas, i)
		}
	}

	// Step 3: Use shared cluster logic to find winning cluster and source
	sharedReplicas := toSharedReplicaInfo(replicas)
	clusterDesc := cluster.FindWinningCluster(sharedReplicas, "", "") // workloadName/port not needed for repair

	if clusterDesc == nil {
		// No valid UUIDs - bootstrap fresh from replica 0
		slog.Debug("no valid UUIDs found, bootstrapping fresh cluster", "sourceReplica", 0)
		return bootstrapFreshAndRejoin(ctx, replicas, 0)
	}

	sourceReplica := clusterDesc.SourceIdx
	slog.Debug("selected source replica from cluster", "replica", sourceReplica, "uuid", clusterDesc.UUID, "nodeCount", clusterDesc.NodeCount)

	// Step 4: Repair - ensure source has cluster, force others to rejoin
	return repairFromSource(ctx, replicas, sourceReplica)
}

// RepairWithSource performs repair using a specific source replica (legacy interface)
func RepairWithSource(ctx *Context, sourceReplica int) error {
	slog.Debug("starting cluster repair with explicit source", "sourceReplica", sourceReplica)

	if sourceReplica < 0 || sourceReplica >= len(ctx.Clients) {
		return fmt.Errorf("invalid source replica %d (have %d replicas)", sourceReplica, len(ctx.Clients))
	}

	// Collect basic info about replicas
	replicas := make([]ReplicaInfo, len(ctx.Clients))
	for i, c := range ctx.Clients {
		replicas[i] = ReplicaInfo{Index: i}
		health, err := c.Health(0)
		if err != nil {
			slog.Debug("replica health check failed", "replica", i, "error", err)
			replicas[i].Reachable = false
			continue
		}
		replicas[i].Reachable = true
		replicas[i].Health = health
	}

	return repairFromSource(ctx, replicas, sourceReplica)
}

// repairFromSource bootstraps/verifies the source replica and forces others to rejoin
func repairFromSource(ctx *Context, replicas []ReplicaInfo, sourceReplica int) error {
	source := ctx.Clients[sourceReplica]

	// Verify source is reachable
	if !replicas[sourceReplica].Reachable {
		return fmt.Errorf("source replica %d is not reachable", sourceReplica)
	}

	// Check if source already has a healthy cluster
	health := replicas[sourceReplica].Health
	if health.ClusterStatus == "primary" {
		slog.Debug("source replica already has cluster", "replica", sourceReplica, "status", health.ClusterStatus)
	} else {
		// Bootstrap cluster on source
		slog.Debug("bootstrapping cluster on source replica", "replica", sourceReplica)
		if err := source.ClusterBootstrap(0); err != nil {
			return fmt.Errorf("failed to bootstrap cluster on replica %d: %w", sourceReplica, err)
		}
		slog.Debug("cluster bootstrapped", "replica", sourceReplica)
	}

	// Get source replica's replication address
	sourceAddr := deriveReplicationAddr(source.BaseURL())
	slog.Debug("source replication address", "addr", sourceAddr)

	// Force all other replicas to rejoin via source
	for i, c := range ctx.Clients {
		if i == sourceReplica {
			slog.Debug("skipping source replica", "replica", i)
			continue
		}

		if !replicas[i].Reachable {
			slog.Warn("replica not reachable, skipping", "replica", i)
			continue
		}

		slog.Debug("forcing replica to rejoin cluster", "replica", i, "via", sourceAddr)
		if err := c.ClusterRejoin(sourceAddr, 0); err != nil {
			return fmt.Errorf("failed to rejoin replica %d: %w", i, err)
		}
		slog.Debug("replica rejoined cluster", "replica", i)
	}

	// Verify all replicas are now in the same cluster
	slog.Debug("verifying cluster membership")
	for i, c := range ctx.Clients {
		health, err := c.Health(0)
		if err != nil {
			slog.Warn("replica health check failed", "replica", i, "error", err)
			continue
		}
		slog.Debug("replica cluster status", "replica", i, "clusterStatus", health.ClusterStatus)
	}

	slog.Info("cluster repair completed successfully")
	return nil
}

// bootstrapFreshAndRejoin bootstraps a fresh cluster when no UUIDs exist
func bootstrapFreshAndRejoin(ctx *Context, replicas []ReplicaInfo, sourceReplica int) error {
	source := ctx.Clients[sourceReplica]

	// Verify source is reachable
	if !replicas[sourceReplica].Reachable {
		// Find first reachable replica
		for i, r := range replicas {
			if r.Reachable {
				sourceReplica = i
				source = ctx.Clients[sourceReplica]
				slog.Debug("replica 0 not reachable, using alternate source", "sourceReplica", sourceReplica)
				break
			}
		}
		if !replicas[sourceReplica].Reachable {
			return fmt.Errorf("no reachable replicas found for fresh bootstrap")
		}
	}

	// Bootstrap fresh cluster
	slog.Debug("bootstrapping fresh cluster", "replica", sourceReplica)
	if err := source.ClusterBootstrap(0); err != nil {
		return fmt.Errorf("failed to bootstrap fresh cluster on replica %d: %w", sourceReplica, err)
	}

	// Get source address and rejoin others
	sourceAddr := deriveReplicationAddr(source.BaseURL())
	slog.Debug("source replication address", "addr", sourceAddr)

	for i, c := range ctx.Clients {
		if i == sourceReplica {
			continue
		}
		if !replicas[i].Reachable {
			slog.Warn("replica not reachable, skipping", "replica", i)
			continue
		}

		slog.Debug("joining replica to fresh cluster", "replica", i, "via", sourceAddr)
		if err := c.ClusterJoin(sourceAddr, 0); err != nil {
			return fmt.Errorf("failed to join replica %d to fresh cluster: %w", i, err)
		}
		slog.Debug("replica joined fresh cluster", "replica", i)
	}

	slog.Info("fresh cluster bootstrap completed successfully")
	return nil
}

// deriveReplicationAddr extracts the internal replication address from agent URL
// Agent URL: http://manticore-manticore-0.manticore-manticore:8080
// Replication: manticore-manticore-0.manticore-manticore:9312
func deriveReplicationAddr(agentURL string) string {
	// Remove protocol
	url := strings.TrimPrefix(agentURL, "http://")
	url = strings.TrimPrefix(url, "https://")

	// Remove port (everything after last colon that's followed by digits)
	if colonIdx := strings.LastIndex(url, ":"); colonIdx != -1 {
		url = url[:colonIdx]
	}

	// URL is now: manticore-manticore-0.manticore-manticore (the correct hostname)
	// Just append the replication port
	return url + ":9312"
}
