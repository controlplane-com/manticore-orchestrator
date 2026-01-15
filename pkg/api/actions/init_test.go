package actions

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/api/client"
	"github.com/controlplane-com/manticore-orchestrator/shared/types"
)

func TestCalculateBackoffWithJitter(t *testing.T) {
	// Test that backoff values are within expected bounds
	tests := []struct {
		name      string
		attempt   int
		minExpect time.Duration
		maxExpect time.Duration
	}{
		{"attempt 1", 1, initBaseDelay * 7 / 10, initBaseDelay * 13 / 10},         // ~2s +/- 30%
		{"attempt 2", 2, initBaseDelay * 2 * 7 / 10, initBaseDelay * 2 * 13 / 10}, // ~4s +/- 30%
		{"attempt 3", 3, initBaseDelay * 4 * 7 / 10, initBaseDelay * 4 * 13 / 10}, // ~8s +/- 30%
		{"attempt 10", 10, initBaseDelay, initMaxDelay * 13 / 10},                 // capped at max
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to check for jitter distribution
			for i := 0; i < 100; i++ {
				result := calculateBackoffWithJitter(tt.attempt)
				if result < tt.minExpect || result > tt.maxExpect {
					t.Errorf("attempt %d: got %v, expected between %v and %v",
						tt.attempt, result, tt.minExpect, tt.maxExpect)
				}
			}
		})
	}
}

func TestCalculateBackoffWithJitter_NeverBelowBase(t *testing.T) {
	for attempt := 1; attempt <= 20; attempt++ {
		for i := 0; i < 50; i++ {
			result := calculateBackoffWithJitter(attempt)
			if result < initBaseDelay {
				t.Errorf("attempt %d iteration %d: got %v, should never be below %v",
					attempt, i, result, initBaseDelay)
			}
		}
	}
}

func TestExtractHostname(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected string
	}{
		{"simple http", "http://localhost:8080", "localhost"},
		{"with path", "http://api.example.com/api/health", "api.example.com"},
		{"https", "https://secure.example.com:443/path", "secure.example.com"},
		{"ip address", "http://192.168.1.1:8080", "192.168.1.1"},
		{"stateful workload format", "http://manticore-manticore-0.manticore-manticore:8080", "manticore-manticore-0.manticore-manticore"},
		{"empty string", "", ""},
		{"invalid url", "not-a-url", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractHostname(tt.url)
			if result != tt.expected {
				t.Errorf("extractHostname(%q) = %q, want %q", tt.url, result, tt.expected)
			}
		})
	}
}

func TestFindFirstReachable(t *testing.T) {
	tests := []struct {
		name     string
		replicas []ReplicaInfo
		expected int
	}{
		{
			name:     "empty slice",
			replicas: []ReplicaInfo{},
			expected: -1,
		},
		{
			name: "first is reachable",
			replicas: []ReplicaInfo{
				{Index: 0, Reachable: true},
				{Index: 1, Reachable: false},
			},
			expected: 0,
		},
		{
			name: "second is first reachable",
			replicas: []ReplicaInfo{
				{Index: 0, Reachable: false},
				{Index: 1, Reachable: true},
				{Index: 2, Reachable: true},
			},
			expected: 1,
		},
		{
			name: "none reachable",
			replicas: []ReplicaInfo{
				{Index: 0, Reachable: false},
				{Index: 1, Reachable: false},
			},
			expected: -1,
		},
		{
			name: "all reachable returns first",
			replicas: []ReplicaInfo{
				{Index: 0, Reachable: true},
				{Index: 1, Reachable: true},
				{Index: 2, Reachable: true},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findFirstReachable(tt.replicas)
			if result != tt.expected {
				t.Errorf("findFirstReachable() = %d, want %d", result, tt.expected)
			}
		})
	}
}

// mockAgentServer creates a test server that simulates agent responses
func mockAgentServer(t *testing.T, healthResponse *types.HealthResponse, tablesResponse []types.TableInfo) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/health":
			if healthResponse != nil {
				json.NewEncoder(w).Encode(healthResponse)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"error":"unavailable"}`))
			}
		case "/api/tables":
			json.NewEncoder(w).Encode(types.TablesResponse{Tables: tablesResponse})
		case "/api/grastate":
			json.NewEncoder(w).Encode(types.GrastateResponse{
				UUID:   "test-uuid",
				Exists: true,
			})
		default:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
}

func TestDiscoverTableSlots(t *testing.T) {
	t.Run("finds slot a from response", func(t *testing.T) {
		// Agent now returns slot directly in TableInfo for base tables
		server := mockAgentServer(t, &types.HealthResponse{ClusterStatus: "primary", NodeState: "synced"}, []types.TableInfo{
			{Name: "products", Slot: "a"},
			{Name: "products_main_a"},
			{Name: "products_delta"},
		})
		defer server.Close()

		clients := []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		}

		result := DiscoverTableSlots(clients, []string{"products"})
		if result["products"] != "a" {
			t.Errorf("DiscoverTableSlots()[products] = %q, want 'a'", result["products"])
		}
	})

	t.Run("finds slot b from response", func(t *testing.T) {
		// Agent now returns slot directly in TableInfo for base tables
		server := mockAgentServer(t, &types.HealthResponse{ClusterStatus: "primary", NodeState: "synced"}, []types.TableInfo{
			{Name: "products", Slot: "b"},
			{Name: "products_main_b"},
			{Name: "products_delta"},
		})
		defer server.Close()

		clients := []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		}

		result := DiscoverTableSlots(clients, []string{"products"})
		if result["products"] != "b" {
			t.Errorf("DiscoverTableSlots()[products] = %q, want 'b'", result["products"])
		}
	})

	t.Run("returns empty slot when table not found", func(t *testing.T) {
		server := mockAgentServer(t, &types.HealthResponse{ClusterStatus: "primary", NodeState: "synced"}, []types.TableInfo{})
		defer server.Close()

		clients := []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		}

		result := DiscoverTableSlots(clients, []string{"products"})
		if result["products"] != "" {
			t.Errorf("DiscoverTableSlots()[products] = %q, want '' (indeterminate)", result["products"])
		}
	})

	t.Run("handles multiple tables with different slots", func(t *testing.T) {
		server := mockAgentServer(t, &types.HealthResponse{ClusterStatus: "primary", NodeState: "synced"}, []types.TableInfo{
			{Name: "products", Slot: "a"},
			{Name: "addresses", Slot: "b"},
			{Name: "products_main_a"},
			{Name: "addresses_main_b"},
		})
		defer server.Close()

		clients := []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		}

		result := DiscoverTableSlots(clients, []string{"products", "addresses"})
		if result["products"] != "a" {
			t.Errorf("DiscoverTableSlots()[products] = %q, want 'a'", result["products"])
		}
		if result["addresses"] != "b" {
			t.Errorf("DiscoverTableSlots()[addresses] = %q, want 'b'", result["addresses"])
		}
	})

	t.Run("returns empty slot for missing tables", func(t *testing.T) {
		server := mockAgentServer(t, &types.HealthResponse{ClusterStatus: "primary", NodeState: "synced"}, []types.TableInfo{
			{Name: "products", Slot: "b"},
			{Name: "products_main_b"},
		})
		defer server.Close()

		clients := []*client.AgentClient{
			client.NewAgentClient(server.URL, "token"),
		}

		result := DiscoverTableSlots(clients, []string{"products", "newTable"})
		if result["products"] != "b" {
			t.Errorf("DiscoverTableSlots()[products] = %q, want 'b'", result["products"])
		}
		if result["newTable"] != "" {
			t.Errorf("DiscoverTableSlots()[newTable] = %q, want '' (indeterminate)", result["newTable"])
		}
	})

	t.Run("skips replicas not in cluster", func(t *testing.T) {
		// First server is not in cluster (empty ClusterStatus)
		notInCluster := mockAgentServer(t, &types.HealthResponse{ClusterStatus: ""}, []types.TableInfo{
			{Name: "products", Slot: "a"}, // Wrong slot - should be ignored
		})
		defer notInCluster.Close()

		// Second server is in cluster with correct data
		inCluster := mockAgentServer(t, &types.HealthResponse{ClusterStatus: "non-primary", NodeState: "synced"}, []types.TableInfo{
			{Name: "products", Slot: "b"}, // Correct slot
		})
		defer inCluster.Close()

		clients := []*client.AgentClient{
			client.NewAgentClient(notInCluster.URL, "token"),
			client.NewAgentClient(inCluster.URL, "token"),
		}

		result := DiscoverTableSlots(clients, []string{"products"})
		if result["products"] != "b" {
			t.Errorf("DiscoverTableSlots()[products] = %q, want 'b' (from cluster member)", result["products"])
		}
	})

	t.Run("returns empty slot when no cluster members available", func(t *testing.T) {
		// Server is not in cluster
		notInCluster := mockAgentServer(t, &types.HealthResponse{ClusterStatus: ""}, []types.TableInfo{
			{Name: "products", Slot: "b"},
		})
		defer notInCluster.Close()

		clients := []*client.AgentClient{
			client.NewAgentClient(notInCluster.URL, "token"),
		}

		result := DiscoverTableSlots(clients, []string{"products"})
		if result["products"] != "" {
			t.Errorf("DiscoverTableSlots()[products] = %q, want '' (indeterminate when no cluster members)", result["products"])
		}
	})
}

func TestInit_BootstrapWhenSafeToBootstrap(t *testing.T) {
	// Create a mock server that returns 503 (pod doesn't exist)
	unavailableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailableServer.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		for i := 0; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(unavailableServer.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 3, nil
	}

	callerInfo := &CallerInfo{
		UUID:            "cluster-uuid",
		Seqno:           100,
		SafeToBootstrap: 1, // This should trigger immediate bootstrap
		Exists:          true,
	}

	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		0, // calling replica
		callerInfo,
		5,  // aliveSinceStart
		60, // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "bootstrap" {
		t.Errorf("Action = %q, want 'bootstrap'", result.Action)
	}
	// Bootstrap action now returns empty TableSlots - agent determines slots locally
	if len(result.TableSlots) != 0 {
		t.Errorf("TableSlots should be empty for bootstrap, got %v", result.TableSlots)
	}
}

func TestInit_BootstrapAfterTimeout(t *testing.T) {
	unavailableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailableServer.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		for i := 0; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(unavailableServer.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 3, nil
	}

	// No safe_to_bootstrap, but aliveSinceStart >= bootstrapTimeout
	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		0,   // calling replica
		nil, // no caller info
		120, // aliveSinceStart (exceeds timeout)
		60,  // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "bootstrap" {
		t.Errorf("Action = %q, want 'bootstrap'", result.Action)
	}
}

func TestInit_WaitBeforeTimeout(t *testing.T) {
	unavailableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailableServer.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		for i := 0; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(unavailableServer.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 3, nil
	}

	// Not safe_to_bootstrap and aliveSinceStart < bootstrapTimeout
	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		0,   // calling replica
		nil, // no caller info (so safe_to_bootstrap=0)
		10,  // aliveSinceStart (less than timeout)
		60,  // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "wait" {
		t.Errorf("Action = %q, want 'wait'", result.Action)
	}
	if result.RetryAfterSeconds != 5 {
		t.Errorf("RetryAfterSeconds = %d, want 5", result.RetryAfterSeconds)
	}
}

func TestInit_JoinExistingCluster(t *testing.T) {
	// Create a healthy replica server
	healthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})
		case "/api/grastate":
			json.NewEncoder(w).Encode(types.GrastateResponse{
				UUID:   "existing-cluster-uuid",
				Seqno:  100,
				Exists: true,
			})
		case "/api/tables":
			json.NewEncoder(w).Encode(types.TablesResponse{
				Tables: []types.TableInfo{
					{Name: "products", Slot: "a"},
					{Name: "products_main_a"},
					{Name: "products_delta"},
				},
			})
		default:
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer healthyServer.Close()

	// Create an unavailable server for the calling replica's slot
	unavailableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailableServer.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		// Replica 0 (calling) uses unavailable server
		clients[0] = client.NewAgentClient(unavailableServer.URL, "token")
		// Replica 1 uses healthy server
		if replicaCount > 1 {
			clients[1] = client.NewAgentClient(healthyServer.URL, "token")
		}
		// Any other replicas use unavailable
		for i := 2; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(unavailableServer.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 2, nil
	}

	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		0,   // calling replica (will be skipped)
		nil, // no caller info
		10,  // aliveSinceStart
		60,  // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "join" {
		t.Errorf("Action = %q, want 'join'", result.Action)
	}
	if result.TableSlots["products"] != "a" {
		t.Errorf("TableSlots[products] = %q, want 'a'", result.TableSlots["products"])
	}
	if result.SourceAddr == "" {
		t.Error("SourceAddr should not be empty")
	}
}

func TestInit_ContextCancellation(t *testing.T) {
	// Create a server that delays responses to allow cancellation to happen
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer slowServer.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		for i := 0; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(slowServer.URL, "token")
		}
		return clients
	}

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Init(
		ctx,
		clientBuilder,
		func() (int, error) { return 3, nil },
		[]TableConfig{},
		0,
		nil,
		10,
		60,
	)

	if err == nil {
		t.Error("Init() should return error when context is cancelled")
	}
	if !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("Error should mention cancellation, got: %v", err)
	}
}

func TestInit_InvalidCallingReplica(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		for i := 0; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(server.URL, "token")
		}
		return clients
	}

	// Use a short timeout context to avoid waiting for all retries
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Calling replica 5 when replicaCount is 3
	_, err := Init(
		ctx,
		clientBuilder,
		func() (int, error) { return 3, nil },
		[]TableConfig{},
		5, // Out of range
		nil,
		10,
		60,
	)

	if err == nil {
		t.Error("Init() should return error for invalid calling replica")
	}
	// Either "out of range" from failed retries or "cancelled" from timeout
	if !strings.Contains(err.Error(), "out of range") && !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("Error should mention out of range or cancelled, got: %v", err)
	}
}

func TestInitResult_JSONMarshaling(t *testing.T) {
	result := InitResult{
		Action:            "join",
		RetryAfterSeconds: 5,
		TableSlots:        map[string]string{"products": "a", "addresses": "b"},
		SourceAddr:        "replica-0:9312",
		ClusterUUID:       "abc-123",
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var unmarshaled InitResult
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Action != result.Action {
		t.Errorf("Action = %q, want %q", unmarshaled.Action, result.Action)
	}
	if unmarshaled.TableSlots["products"] != result.TableSlots["products"] {
		t.Errorf("TableSlots[products] = %q, want %q", unmarshaled.TableSlots["products"], result.TableSlots["products"])
	}
	if unmarshaled.TableSlots["addresses"] != result.TableSlots["addresses"] {
		t.Errorf("TableSlots[addresses] = %q, want %q", unmarshaled.TableSlots["addresses"], result.TableSlots["addresses"])
	}
}

func TestCallerInfo_ZeroValue(t *testing.T) {
	info := CallerInfo{}
	if info.SafeToBootstrap != 0 {
		t.Errorf("Zero SafeToBootstrap should be 0, got %d", info.SafeToBootstrap)
	}
	if info.Exists {
		t.Error("Zero Exists should be false")
	}
}

// mockNoClusterServer creates a server that responds as healthy but not in any cluster
func mockNoClusterServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			// Healthy but not in cluster (no valid ClusterStatus)
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "", // Not in any cluster
				NodeState:     "",
			})
		case "/api/grastate":
			// No valid grastate
			json.NewEncoder(w).Encode(types.GrastateResponse{
				UUID:   "",
				Exists: false,
			})
		case "/api/tables":
			json.NewEncoder(w).Encode(types.TablesResponse{Tables: []types.TableInfo{}})
		default:
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
}

func TestInit_BootstrapLeaderElection_CallerIsLowest(t *testing.T) {
	// All replicas are available but none have a cluster
	// Caller (replica 0) is the lowest index → should bootstrap
	server := mockNoClusterServer()
	defer server.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		for i := 0; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(server.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 3, nil
	}

	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		0,   // calling replica (lowest)
		nil, // no caller info
		10,  // aliveSinceStart
		60,  // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "bootstrap" {
		t.Errorf("Action = %q, want 'bootstrap' (caller is lowest available)", result.Action)
	}
	// Bootstrap action now returns empty TableSlots - agent determines slots locally
	if len(result.TableSlots) != 0 {
		t.Errorf("TableSlots should be empty for bootstrap, got %v", result.TableSlots)
	}
}

func TestInit_BootstrapLeaderElection_CallerNotLowest(t *testing.T) {
	// All replicas are available but none have a cluster
	// Caller (replica 2) is NOT the lowest index → should wait
	server := mockNoClusterServer()
	defer server.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		for i := 0; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(server.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 3, nil
	}

	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		2,   // calling replica (NOT lowest - 0 and 1 are available)
		nil, // no caller info
		10,  // aliveSinceStart
		60,  // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "wait" {
		t.Errorf("Action = %q, want 'wait' (caller is not lowest available)", result.Action)
	}
	if result.RetryAfterSeconds != 5 {
		t.Errorf("RetryAfterSeconds = %d, want 5", result.RetryAfterSeconds)
	}
}

func TestInit_BootstrapLeaderElection_LowestUnavailable(t *testing.T) {
	// Replica 0 is unavailable (503), replicas 1 and 2 are available but no cluster
	// Caller (replica 1) is the lowest AVAILABLE index → should bootstrap
	noClusterServer := mockNoClusterServer()
	defer noClusterServer.Close()

	unavailableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailableServer.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		// Replica 0: unavailable
		clients[0] = client.NewAgentClient(unavailableServer.URL, "token")
		// Replicas 1+: available but no cluster
		for i := 1; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(noClusterServer.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 3, nil
	}

	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		1,   // calling replica (lowest AVAILABLE since 0 is unavailable)
		nil, // no caller info
		10,  // aliveSinceStart
		60,  // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "bootstrap" {
		t.Errorf("Action = %q, want 'bootstrap' (caller is lowest available)", result.Action)
	}
}

func TestInit_BootstrapLeaderElection_MiddleReplica(t *testing.T) {
	// Replica 0 is unavailable, replicas 1 and 2 are available but no cluster
	// Caller (replica 2) is NOT the lowest available → should wait for replica 1
	noClusterServer := mockNoClusterServer()
	defer noClusterServer.Close()

	unavailableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailableServer.Close()

	clientBuilder := func(replicaCount int) []*client.AgentClient {
		clients := make([]*client.AgentClient, replicaCount)
		// Replica 0: unavailable
		clients[0] = client.NewAgentClient(unavailableServer.URL, "token")
		// Replicas 1+: available but no cluster
		for i := 1; i < replicaCount; i++ {
			clients[i] = client.NewAgentClient(noClusterServer.URL, "token")
		}
		return clients
	}

	replicaCountFetcher := func() (int, error) {
		return 3, nil
	}

	result, err := Init(
		context.Background(),
		clientBuilder,
		replicaCountFetcher,
		[]TableConfig{{Name: "products"}},
		2,   // calling replica (NOT lowest available - 1 is available and lower)
		nil, // no caller info
		10,  // aliveSinceStart
		60,  // bootstrapTimeout
	)

	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if result.Action != "wait" {
		t.Errorf("Action = %q, want 'wait' (replica 1 should bootstrap, not 2)", result.Action)
	}
}

func TestFilterAvailableClients_ReturnsIndices(t *testing.T) {
	// Create servers
	availableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.HealthResponse{Status: "ok"})
	}))
	defer availableServer.Close()

	unavailableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailableServer.Close()

	clients := []*client.AgentClient{
		client.NewAgentClient(availableServer.URL, "token"),   // 0: available
		client.NewAgentClient(unavailableServer.URL, "token"), // 1: unavailable
		client.NewAgentClient(availableServer.URL, "token"),   // 2: available (caller)
		client.NewAgentClient(availableServer.URL, "token"),   // 3: available
	}

	// Filter with replica 2 as caller (excluded)
	filtered, indices, onlyCallerExists := filterAvailableClients(clients, 2)

	if onlyCallerExists {
		t.Error("onlyCallerExists should be false")
	}

	// Should have filtered clients for indices 0 and 3 (1 unavailable, 2 is caller)
	if len(filtered) != 2 {
		t.Errorf("len(filtered) = %d, want 2", len(filtered))
	}

	if len(indices) != 2 {
		t.Errorf("len(indices) = %d, want 2", len(indices))
	}

	// Check indices are correct
	expectedIndices := []int{0, 3}
	for i, expected := range expectedIndices {
		if i >= len(indices) || indices[i] != expected {
			t.Errorf("indices[%d] = %d, want %d", i, indices[i], expected)
		}
	}
}
