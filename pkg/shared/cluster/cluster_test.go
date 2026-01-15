package cluster

import "testing"

func TestFindWinningCluster_IgnoresUnhealthyNodes(t *testing.T) {
	replicas := []ReplicaInfo{
		{Index: 0, HasValidUUID: true, UUID: "uuid-a", Seqno: 100, ClusterStatus: "non-primary"},
		{Index: 1, HasValidUUID: true, UUID: "uuid-a", Seqno: 50, ClusterStatus: "primary"},
	}

	result := FindWinningCluster(replicas, "workload", "9312")

	if result == nil {
		t.Fatal("expected cluster, got nil")
	}
	if result.SourceIdx != 1 {
		t.Errorf("expected source index 1 (healthy node), got %d", result.SourceIdx)
	}
}

func TestFindWinningCluster_ReturnsNilWhenNoHealthyNodes(t *testing.T) {
	replicas := []ReplicaInfo{
		{Index: 0, HasValidUUID: true, UUID: "uuid-a", Seqno: 100, ClusterStatus: "non-primary"},
		{Index: 1, HasValidUUID: true, UUID: "uuid-a", Seqno: 50, ClusterStatus: "donor"},
	}

	result := FindWinningCluster(replicas, "workload", "9312")

	if result != nil {
		t.Errorf("expected nil when no healthy nodes, got %+v", result)
	}
}

func TestFindWinningCluster_SelectsHighestSeqnoAmongHealthy(t *testing.T) {
	replicas := []ReplicaInfo{
		{Index: 0, HasValidUUID: true, UUID: "uuid-a", Seqno: 100, ClusterStatus: "non-primary"}, // unhealthy, highest seqno
		{Index: 1, HasValidUUID: true, UUID: "uuid-a", Seqno: 80, ClusterStatus: "primary"},      // healthy
		{Index: 2, HasValidUUID: true, UUID: "uuid-a", Seqno: 90, ClusterStatus: "primary"},      // healthy, highest among healthy
	}

	result := FindWinningCluster(replicas, "workload", "9312")

	if result == nil {
		t.Fatal("expected cluster, got nil")
	}
	if result.SourceIdx != 2 {
		t.Errorf("expected source index 2 (highest seqno among healthy), got %d", result.SourceIdx)
	}
}

func TestFindWinningClusterWithPreference_IgnoresUnhealthyNodes(t *testing.T) {
	replicas := []ReplicaInfo{
		{Index: 0, HasValidUUID: true, UUID: "uuid-a", Seqno: 100, ClusterStatus: "non-primary"},
		{Index: 1, HasValidUUID: true, UUID: "uuid-a", Seqno: 50, ClusterStatus: "primary"},
	}

	result := FindWinningClusterWithPreference(replicas, "workload", "9312", "uuid-a")

	if result == nil {
		t.Fatal("expected cluster, got nil")
	}
	if result.SourceIdx != 1 {
		t.Errorf("expected source index 1 (healthy node), got %d", result.SourceIdx)
	}
}

func TestFindWinningCluster_CountsOnlyHealthyForWinner(t *testing.T) {
	// uuid-a has 2 nodes but only 1 healthy
	// uuid-b has 2 nodes and both healthy
	// uuid-b should win because it has more healthy nodes
	replicas := []ReplicaInfo{
		{Index: 0, HasValidUUID: true, UUID: "uuid-a", Seqno: 100, ClusterStatus: "primary"},
		{Index: 1, HasValidUUID: true, UUID: "uuid-a", Seqno: 90, ClusterStatus: "non-primary"},
		{Index: 2, HasValidUUID: true, UUID: "uuid-b", Seqno: 80, ClusterStatus: "primary"},
		{Index: 3, HasValidUUID: true, UUID: "uuid-b", Seqno: 70, ClusterStatus: "primary"},
	}

	result := FindWinningCluster(replicas, "workload", "9312")

	if result == nil {
		t.Fatal("expected cluster, got nil")
	}
	if result.UUID != "uuid-b" {
		t.Errorf("expected uuid-b (more healthy nodes), got %s", result.UUID)
	}
	if result.SourceIdx != 2 {
		t.Errorf("expected source index 2 (highest seqno in uuid-b), got %d", result.SourceIdx)
	}
}

func TestBuildReplicationAddr(t *testing.T) {
	tests := []struct {
		workload string
		idx      int
		port     string
		want     string
	}{
		{"manticore", 0, "9312", "manticore-0.manticore:9312"},
		{"search", 2, "9312", "search-2.search:9312"},
	}

	for _, tt := range tests {
		got := BuildReplicationAddr(tt.workload, tt.idx, tt.port)
		if got != tt.want {
			t.Errorf("BuildReplicationAddr(%q, %d, %q) = %q, want %q",
				tt.workload, tt.idx, tt.port, got, tt.want)
		}
	}
}
