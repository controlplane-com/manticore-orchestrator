package cluster

import (
	"math"
)

// ClusterDesc describes a cluster for joining
type ClusterDesc struct {
	UUID       string `json:"uuid"`
	SourceAddr string `json:"sourceAddr"` // e.g., "workload-0.workload:9312"
	SourceIdx  int    `json:"sourceIdx"`
	NodeCount  int    `json:"nodeCount"`
}

// ClusterGroup represents a group of replicas sharing a UUID
type ClusterGroup struct {
	UUID      string `json:"uuid"`
	Replicas  []int  `json:"replicas"`
	NodeCount int    `json:"nodeCount"`
}

// ReplicaInfo holds replica state for cluster analysis
type ReplicaInfo struct {
	Index         int
	Reachable     bool
	ClusterStatus string
	UUID          string
	Seqno         int64
	HasValidUUID  bool
}

// FindWinningCluster analyzes replicas and returns the cluster to join.
// Algorithm:
//  1. Group replicas by UUID (excluding invalid UUIDs)
//  2. Find winning group (most nodes, or oldest seqno for ties)
//  3. Within winning group, find node with highest seqno → source node
//
// Returns nil if no valid cluster found (fresh start scenario).
func FindWinningCluster(replicas []ReplicaInfo, workloadName, replicationPort string) *ClusterDesc {
	// Group replicas by UUID (only include healthy nodes with ClusterStatus == "primary")
	uuidGroups := make(map[string][]int) // uuid -> list of replica indices
	for i, r := range replicas {
		if r.HasValidUUID && r.ClusterStatus == "primary" {
			uuidGroups[r.UUID] = append(uuidGroups[r.UUID], i)
		}
	}

	if len(uuidGroups) == 0 {
		// No healthy nodes with valid UUIDs - fresh cluster scenario
		return nil
	}

	// Find max group size
	maxSize := 0
	for _, nodes := range uuidGroups {
		if len(nodes) > maxSize {
			maxSize = len(nodes)
		}
	}

	// Get all groups with max size (potential ties)
	tiedGroups := make(map[string][]int)
	for uuid, nodes := range uuidGroups {
		if len(nodes) == maxSize {
			tiedGroups[uuid] = nodes
		}
	}

	var winningUUID string
	var winningNodes []int

	if len(tiedGroups) == 1 {
		// Single winner by node count
		for uuid, nodes := range tiedGroups {
			winningUUID = uuid
			winningNodes = nodes
		}
	} else {
		// Tie-breaker: find group with the oldest (lowest) minimum seqno
		// This represents the "oldest established" cluster
		var oldestMinSeqno int64 = math.MaxInt64
		for uuid, nodes := range tiedGroups {
			// Find min seqno in this group
			var minSeqno int64 = math.MaxInt64
			for _, nodeIdx := range nodes {
				if replicas[nodeIdx].Seqno < minSeqno {
					minSeqno = replicas[nodeIdx].Seqno
				}
			}

			if minSeqno < oldestMinSeqno {
				oldestMinSeqno = minSeqno
				winningUUID = uuid
				winningNodes = nodes
			}
		}
	}

	// Within winning group, find node with highest seqno (most up-to-date)
	var highestSeqno int64 = -1
	var sourceIdx int
	for _, nodeIdx := range winningNodes {
		seqno := replicas[nodeIdx].Seqno
		if seqno > highestSeqno {
			highestSeqno = seqno
			sourceIdx = nodeIdx
		}
	}

	// Build source address
	sourceAddr := BuildReplicationAddr(workloadName, sourceIdx, replicationPort)

	return &ClusterDesc{
		UUID:       winningUUID,
		SourceAddr: sourceAddr,
		SourceIdx:  sourceIdx,
		NodeCount:  len(winningNodes),
	}
}

// FindWinningClusterWithPreference is like FindWinningCluster but prefers a specific UUID.
// If preferredUUID matches a group with max node count, it wins even in ties.
// This is used when the calling agent has an existing cluster UUID that we want to prefer.
func FindWinningClusterWithPreference(replicas []ReplicaInfo, workloadName, replicationPort, preferredUUID string) *ClusterDesc {
	// Group replicas by UUID (only include healthy nodes with ClusterStatus == "primary")
	uuidGroups := make(map[string][]int) // uuid -> list of replica indices
	for i, r := range replicas {
		if r.HasValidUUID && r.ClusterStatus == "primary" {
			uuidGroups[r.UUID] = append(uuidGroups[r.UUID], i)
		}
	}

	if len(uuidGroups) == 0 {
		// No healthy nodes with valid UUIDs - fresh cluster scenario
		return nil
	}

	// Find max group size
	maxSize := 0
	for _, nodes := range uuidGroups {
		if len(nodes) > maxSize {
			maxSize = len(nodes)
		}
	}

	// Get all groups with max size (potential ties)
	tiedGroups := make(map[string][]int)
	for uuid, nodes := range uuidGroups {
		if len(nodes) == maxSize {
			tiedGroups[uuid] = nodes
		}
	}

	var winningUUID string
	var winningNodes []int

	// If preferred UUID is in the tied groups, it wins
	if preferredUUID != "" {
		if nodes, ok := tiedGroups[preferredUUID]; ok {
			winningUUID = preferredUUID
			winningNodes = nodes
		}
	}

	// If no winner yet, use standard logic
	if winningUUID == "" {
		if len(tiedGroups) == 1 {
			// Single winner by node count
			for uuid, nodes := range tiedGroups {
				winningUUID = uuid
				winningNodes = nodes
			}
		} else {
			// Tie-breaker: find group with the oldest (lowest) minimum seqno
			var oldestMinSeqno int64 = math.MaxInt64
			for uuid, nodes := range tiedGroups {
				// Find min seqno in this group
				var minSeqno int64 = math.MaxInt64
				for _, nodeIdx := range nodes {
					if replicas[nodeIdx].Seqno < minSeqno {
						minSeqno = replicas[nodeIdx].Seqno
					}
				}

				if minSeqno < oldestMinSeqno {
					oldestMinSeqno = minSeqno
					winningUUID = uuid
					winningNodes = nodes
				}
			}
		}
	}

	// Within winning group, find node with highest seqno (most up-to-date)
	var highestSeqno int64 = -1
	var sourceIdx int
	for _, nodeIdx := range winningNodes {
		seqno := replicas[nodeIdx].Seqno
		if seqno > highestSeqno {
			highestSeqno = seqno
			sourceIdx = nodeIdx
		}
	}

	// Build source address
	sourceAddr := BuildReplicationAddr(workloadName, sourceIdx, replicationPort)

	return &ClusterDesc{
		UUID:       winningUUID,
		SourceAddr: sourceAddr,
		SourceIdx:  sourceIdx,
		NodeCount:  len(winningNodes),
	}
}

// FindAllClusterGroups returns all UUID groups for split-brain detection.
// Used by the UI to display cluster partition information.
func FindAllClusterGroups(replicas []ReplicaInfo) []ClusterGroup {
	// Group replicas by UUID (only include healthy nodes with ClusterStatus == "primary")
	uuidGroups := make(map[string][]int)
	for i, r := range replicas {
		if r.HasValidUUID && r.ClusterStatus == "primary" {
			uuidGroups[r.UUID] = append(uuidGroups[r.UUID], i)
		}
	}

	var groups []ClusterGroup
	for uuid, nodes := range uuidGroups {
		groups = append(groups, ClusterGroup{
			UUID:      uuid,
			Replicas:  nodes,
			NodeCount: len(nodes),
		})
	}
	return groups
}

// BuildReplicationAddr constructs the replication address for a replica
// Format: workloadName-index.workloadName:port
func BuildReplicationAddr(workloadName string, replicaIdx int, port string) string {
	return workloadName + "-" + itoa(replicaIdx) + "." + workloadName + ":" + port
}

// itoa converts int to string without importing strconv
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	if i < 0 {
		return "-" + itoa(-i)
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
