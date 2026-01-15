package actions

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/client"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
)

func TestDeriveReplicationAddr(t *testing.T) {
	tests := []struct {
		name     string
		agentURL string
		expected string
	}{
		{
			name:     "standard agent URL",
			agentURL: "http://manticore-manticore-0.manticore-manticore:8080",
			expected: "manticore-manticore-0.manticore-manticore:9312",
		},
		{
			name:     "replica index 1",
			agentURL: "http://manticore-manticore-1.manticore-manticore:8080",
			expected: "manticore-manticore-1.manticore-manticore:9312",
		},
		{
			name:     "https URL",
			agentURL: "https://manticore-manticore-2.manticore-manticore:8080",
			expected: "manticore-manticore-2.manticore-manticore:9312",
		},
		{
			name:     "different workload name",
			agentURL: "http://search-search-0.search-search:8080",
			expected: "search-search-0.search-search:9312",
		},
		{
			name:     "simple hostname",
			agentURL: "http://localhost:8080",
			expected: "localhost:9312",
		},
		{
			name:     "no port in URL",
			agentURL: "http://manticore-manticore-0.manticore-manticore",
			expected: "manticore-manticore-0.manticore-manticore:9312",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deriveReplicationAddr(tt.agentURL)
			if result != tt.expected {
				t.Errorf("deriveReplicationAddr(%q) = %q, want %q",
					tt.agentURL, result, tt.expected)
			}
		})
	}
}

func TestToSharedReplicaInfo(t *testing.T) {
	tests := []struct {
		name     string
		replicas []ReplicaInfo
		want     int // expected length
	}{
		{
			name:     "empty slice",
			replicas: []ReplicaInfo{},
			want:     0,
		},
		{
			name: "single replica with all fields",
			replicas: []ReplicaInfo{
				{
					Index:        0,
					Reachable:    true,
					HasValidUUID: true,
					Health:       &types.HealthResponse{ClusterStatus: "primary", NodeState: "synced"},
					Grastate:     &types.GrastateResponse{UUID: "test-uuid", Seqno: 100},
				},
			},
			want: 1,
		},
		{
			name: "multiple replicas mixed states",
			replicas: []ReplicaInfo{
				{Index: 0, Reachable: true, HasValidUUID: true},
				{Index: 1, Reachable: false, HasValidUUID: false},
				{Index: 2, Reachable: true, HasValidUUID: true},
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toSharedReplicaInfo(tt.replicas)
			if len(result) != tt.want {
				t.Errorf("toSharedReplicaInfo() returned %d items, want %d", len(result), tt.want)
			}

			// Verify fields are correctly copied
			for i, r := range tt.replicas {
				if result[i].Index != r.Index {
					t.Errorf("result[%d].Index = %d, want %d", i, result[i].Index, r.Index)
				}
				if result[i].Reachable != r.Reachable {
					t.Errorf("result[%d].Reachable = %v, want %v", i, result[i].Reachable, r.Reachable)
				}
				if result[i].HasValidUUID != r.HasValidUUID {
					t.Errorf("result[%d].HasValidUUID = %v, want %v", i, result[i].HasValidUUID, r.HasValidUUID)
				}
				if r.Health != nil && result[i].ClusterStatus != r.Health.ClusterStatus {
					t.Errorf("result[%d].ClusterStatus = %q, want %q", i, result[i].ClusterStatus, r.Health.ClusterStatus)
				}
				if r.Grastate != nil {
					if result[i].UUID != r.Grastate.UUID {
						t.Errorf("result[%d].UUID = %q, want %q", i, result[i].UUID, r.Grastate.UUID)
					}
					if result[i].Seqno != r.Grastate.Seqno {
						t.Errorf("result[%d].Seqno = %d, want %d", i, result[i].Seqno, r.Grastate.Seqno)
					}
				}
			}
		})
	}
}

func TestToSharedReplicaInfo_NilFields(t *testing.T) {
	replicas := []ReplicaInfo{
		{
			Index:     0,
			Reachable: true,
			Health:    nil, // nil health
			Grastate:  nil, // nil grastate
		},
	}

	result := toSharedReplicaInfo(replicas)

	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}
	if result[0].ClusterStatus != "" {
		t.Errorf("ClusterStatus should be empty for nil Health, got %q", result[0].ClusterStatus)
	}
	if result[0].UUID != "" {
		t.Errorf("UUID should be empty for nil Grastate, got %q", result[0].UUID)
	}
}

func TestReplicaInfo_ZeroValue(t *testing.T) {
	info := ReplicaInfo{}

	if info.Index != 0 {
		t.Errorf("Zero Index should be 0, got %d", info.Index)
	}
	if info.Reachable {
		t.Error("Zero Reachable should be false")
	}
	if info.HasValidUUID {
		t.Error("Zero HasValidUUID should be false")
	}
	if info.Health != nil {
		t.Error("Zero Health should be nil")
	}
	if info.Grastate != nil {
		t.Error("Zero Grastate should be nil")
	}
}

func TestGetReplicaInfo_HealthCheckFailure(t *testing.T) {
	// Server that returns 500 for health check
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "internal error"}`))
	}))
	defer server.Close()

	c := client.NewAgentClient(server.URL, "test-token")
	info := getReplicaInfo(0, c)

	if info.Index != 0 {
		t.Errorf("Index = %d, want 0", info.Index)
	}
	if info.Reachable {
		t.Error("Reachable should be false when health check fails")
	}
	if info.Health != nil {
		t.Error("Health should be nil when health check fails")
	}
}

func TestGetReplicaInfo_HealthyNoGrastate(t *testing.T) {
	// Server that returns healthy but fails grastate
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if r.URL.Path == "/api/health" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})
			return
		}
		if r.URL.Path == "/api/grastate" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "grastate error"}`))
			return
		}
	}))
	defer server.Close()

	c := client.NewAgentClient(server.URL, "test-token")
	info := getReplicaInfo(1, c)

	if info.Index != 1 {
		t.Errorf("Index = %d, want 1", info.Index)
	}
	if !info.Reachable {
		t.Error("Reachable should be true when health check succeeds")
	}
	if info.Health == nil {
		t.Fatal("Health should not be nil")
	}
	if info.Health.ClusterStatus != "primary" {
		t.Errorf("ClusterStatus = %q, want 'primary'", info.Health.ClusterStatus)
	}
	if info.Grastate != nil {
		t.Error("Grastate should be nil when grastate check fails")
	}
	if info.HasValidUUID {
		t.Error("HasValidUUID should be false when no grastate")
	}
}

func TestGetReplicaInfo_FullSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/health" {
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})
			return
		}
		if r.URL.Path == "/api/grastate" {
			json.NewEncoder(w).Encode(types.GrastateResponse{
				UUID:            "valid-cluster-uuid",
				Seqno:           100,
				SafeToBootstrap: 1,
				Exists:          true,
			})
			return
		}
	}))
	defer server.Close()

	c := client.NewAgentClient(server.URL, "test-token")
	info := getReplicaInfo(2, c)

	if info.Index != 2 {
		t.Errorf("Index = %d, want 2", info.Index)
	}
	if !info.Reachable {
		t.Error("Reachable should be true")
	}
	if info.Health == nil {
		t.Fatal("Health should not be nil")
	}
	if info.Grastate == nil {
		t.Fatal("Grastate should not be nil")
	}
	if !info.HasValidUUID {
		t.Error("HasValidUUID should be true for valid UUID")
	}
	if info.Grastate.UUID != "valid-cluster-uuid" {
		t.Errorf("UUID = %q, want 'valid-cluster-uuid'", info.Grastate.UUID)
	}
}

func TestGetReplicaInfo_NullUUID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/health" {
			json.NewEncoder(w).Encode(types.HealthResponse{Status: "ok"})
			return
		}
		if r.URL.Path == "/api/grastate" {
			json.NewEncoder(w).Encode(types.GrastateResponse{
				UUID:   "00000000-0000-0000-0000-000000000000",
				Seqno:  0,
				Exists: true,
			})
			return
		}
	}))
	defer server.Close()

	c := client.NewAgentClient(server.URL, "test-token")
	info := getReplicaInfo(0, c)

	if info.HasValidUUID {
		t.Error("HasValidUUID should be false for null UUID")
	}
}

func TestGetReplicaInfo_EmptyUUID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/health" {
			json.NewEncoder(w).Encode(types.HealthResponse{Status: "ok"})
			return
		}
		if r.URL.Path == "/api/grastate" {
			json.NewEncoder(w).Encode(types.GrastateResponse{
				UUID:   "",
				Exists: true,
			})
			return
		}
	}))
	defer server.Close()

	c := client.NewAgentClient(server.URL, "test-token")
	info := getReplicaInfo(0, c)

	if info.HasValidUUID {
		t.Error("HasValidUUID should be false for empty UUID")
	}
}

func TestGetReplicaInfo_GrastateNotExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/health" {
			json.NewEncoder(w).Encode(types.HealthResponse{Status: "ok"})
			return
		}
		if r.URL.Path == "/api/grastate" {
			json.NewEncoder(w).Encode(types.GrastateResponse{
				UUID:   "some-uuid",
				Exists: false,
			})
			return
		}
	}))
	defer server.Close()

	c := client.NewAgentClient(server.URL, "test-token")
	info := getReplicaInfo(0, c)

	if info.HasValidUUID {
		t.Error("HasValidUUID should be false when Exists is false")
	}
}

func TestRepairWithSource_InvalidSourceReplica(t *testing.T) {
	ctx := &Context{
		Clients: []*client.AgentClient{
			client.NewAgentClient("http://replica-0:8080", "token"),
			client.NewAgentClient("http://replica-1:8080", "token"),
		},
	}

	tests := []struct {
		name          string
		sourceReplica int
	}{
		{"negative index", -1},
		{"index out of range", 5},
		{"exactly at length", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RepairWithSource(ctx, tt.sourceReplica)
			if err == nil {
				t.Error("RepairWithSource should return error for invalid source replica")
			}
		})
	}
}

func TestRepairWithSource_SourceNotReachable(t *testing.T) {
	// Server that returns 500 (health check fails)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		},
	}

	err := RepairWithSource(ctx, 0)
	if err == nil {
		t.Error("RepairWithSource should return error when source is not reachable")
	}
}

func TestRepairWithSource_SourceAlreadyPrimary(t *testing.T) {
	bootstrapCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/health" {
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})
			return
		}
		if r.URL.Path == "/api/cluster/bootstrap" {
			bootstrapCalled = true
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
	}))
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		},
	}

	err := RepairWithSource(ctx, 0)
	if err != nil {
		t.Errorf("RepairWithSource error: %v", err)
	}
	if bootstrapCalled {
		t.Error("Bootstrap should not be called when source is already primary")
	}
}

func TestRepairWithSource_BootstrapNeeded(t *testing.T) {
	bootstrapCalled := false
	healthCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/health" {
			healthCalls++
			// First call: not in cluster, subsequent: primary
			status := "unknown"
			if healthCalls > 1 {
				status = "primary"
			}
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: status,
			})
			return
		}
		if r.URL.Path == "/api/cluster/bootstrap" {
			bootstrapCalled = true
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
	}))
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		},
	}

	err := RepairWithSource(ctx, 0)
	if err != nil {
		t.Errorf("RepairWithSource error: %v", err)
	}
	if !bootstrapCalled {
		t.Error("Bootstrap should be called when source is not primary")
	}
}
