package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
)

func TestNewAgentClient(t *testing.T) {
	client := NewAgentClient("http://localhost:8080", "test-token")

	if client == nil {
		t.Fatal("NewAgentClient() returned nil")
	}
	if client.baseURL != "http://localhost:8080" {
		t.Errorf("baseURL = %q, want %q", client.baseURL, "http://localhost:8080")
	}
	if client.authToken != "test-token" {
		t.Errorf("authToken = %q, want %q", client.authToken, "test-token")
	}
}

func TestAgentClient_BaseURL(t *testing.T) {
	client := NewAgentClient("http://example.com:8080", "token")
	if client.BaseURL() != "http://example.com:8080" {
		t.Errorf("BaseURL() = %q, want %q", client.BaseURL(), "http://example.com:8080")
	}
}

func TestAgentClient_Health(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/health" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != "GET" {
			t.Errorf("unexpected method: %s", r.Method)
		}
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			t.Errorf("missing or invalid Authorization header: %s", authHeader)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.HealthResponse{
			Status:        "ok",
			ClusterStatus: "primary",
			NodeState:     "synced",
		})
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "test-token")
	resp, err := client.Health(0)

	if err != nil {
		t.Fatalf("Health() error: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want 'ok'", resp.Status)
	}
	if resp.ClusterStatus != "primary" {
		t.Errorf("ClusterStatus = %q, want 'primary'", resp.ClusterStatus)
	}
}

func TestAgentClient_HealthProbe_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.HealthResponse{
			Status:        "ok",
			ClusterStatus: "primary",
			NodeState:     "synced",
		})
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	resp, err := client.HealthProbe()

	if err != nil {
		t.Fatalf("HealthProbe() error: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want 'ok'", resp.Status)
	}
}

func TestAgentClient_HealthProbe_503Detection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"service unavailable"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	_, err := client.HealthProbe()

	if err == nil {
		t.Error("HealthProbe() should return error for 503")
	}
	if !strings.Contains(err.Error(), "503") && !strings.Contains(err.Error(), "unavailable") {
		t.Errorf("Error should mention 503 or unavailable, got: %v", err)
	}
}

func TestAgentClient_HealthProbe_4xxError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	_, err := client.HealthProbe()

	if err == nil {
		t.Error("HealthProbe() should return error for 401")
	}
}

func TestAgentClient_Grastate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/grastate" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.GrastateResponse{
			UUID:            "cluster-uuid-123",
			Seqno:           100,
			SafeToBootstrap: 1,
			Exists:          true,
		})
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	resp, err := client.Grastate(0)

	if err != nil {
		t.Fatalf("Grastate() error: %v", err)
	}
	if resp.UUID != "cluster-uuid-123" {
		t.Errorf("UUID = %q, want 'cluster-uuid-123'", resp.UUID)
	}
	if resp.Seqno != 100 {
		t.Errorf("Seqno = %d, want 100", resp.Seqno)
	}
	if resp.SafeToBootstrap != 1 {
		t.Errorf("SafeToBootstrap = %d, want 1", resp.SafeToBootstrap)
	}
}

func TestAgentClient_ListTables(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tables" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.TablesResponse{
			Tables: []types.TableInfo{
				{Name: "products_main_a", Type: "rt", InCluster: true},
				{Name: "products_delta", Type: "rt", InCluster: true},
				{Name: "products", Type: "distributed", InCluster: false},
			},
		})
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	tables, err := client.ListTables(0)

	if err != nil {
		t.Fatalf("ListTables() error: %v", err)
	}
	if len(tables) != 3 {
		t.Fatalf("got %d tables, want 3", len(tables))
	}
	if tables[0].Name != "products_main_a" {
		t.Errorf("tables[0].Name = %q, want 'products_main_a'", tables[0].Name)
	}
}

func TestAgentClient_CreateTable(t *testing.T) {
	var receivedTable string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/table/create" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Errorf("unexpected method: %s", r.Method)
		}

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		receivedTable = body["table"]

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	err := client.CreateTable("test_table", 0)

	if err != nil {
		t.Fatalf("CreateTable() error: %v", err)
	}
	if receivedTable != "test_table" {
		t.Errorf("received table = %q, want 'test_table'", receivedTable)
	}
}

func TestAgentClient_DropTable(t *testing.T) {
	var receivedTable string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/table/drop" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		receivedTable = body["table"]

		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	err := client.DropTable("old_table", 0)

	if err != nil {
		t.Fatalf("DropTable() error: %v", err)
	}
	if receivedTable != "old_table" {
		t.Errorf("received table = %q, want 'old_table'", receivedTable)
	}
}

func TestAgentClient_ClusterBootstrap(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/cluster/bootstrap" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		called = true
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	err := client.ClusterBootstrap(0)

	if err != nil {
		t.Fatalf("ClusterBootstrap() error: %v", err)
	}
	if !called {
		t.Error("ClusterBootstrap() did not call server")
	}
}

func TestAgentClient_ClusterJoin(t *testing.T) {
	var receivedAddr string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/cluster/join" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		receivedAddr = body["sourceAddr"]

		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	err := client.ClusterJoin("replica-0:9312", 0)

	if err != nil {
		t.Fatalf("ClusterJoin() error: %v", err)
	}
	if receivedAddr != "replica-0:9312" {
		t.Errorf("received sourceAddr = %q, want 'replica-0:9312'", receivedAddr)
	}
}

func TestAgentClient_ClusterRejoin(t *testing.T) {
	var receivedAddr string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/cluster/rejoin" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		receivedAddr = body["sourceAddr"]

		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	err := client.ClusterRejoin("replica-1:9312", 0)

	if err != nil {
		t.Fatalf("ClusterRejoinHandler() error: %v", err)
	}
	if receivedAddr != "replica-1:9312" {
		t.Errorf("received sourceAddr = %q, want 'replica-1:9312'", receivedAddr)
	}
}

func TestAgentClient_ClusterAdd(t *testing.T) {
	var receivedTable string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/cluster/add" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		receivedTable = body["table"]

		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	err := client.ClusterAdd("products_delta", 0)

	if err != nil {
		t.Fatalf("ClusterAdd() error: %v", err)
	}
	if receivedTable != "products_delta" {
		t.Errorf("received table = %q, want 'products_delta'", receivedTable)
	}
}

func TestAgentClient_CreateDistributed(t *testing.T) {
	var receivedDistributed string
	var receivedLocals []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/distributed/create" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		receivedDistributed = body["distributed"].(string)
		for _, l := range body["locals"].([]interface{}) {
			receivedLocals = append(receivedLocals, l.(string))
		}

		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	err := client.CreateDistributed("products", []string{"products_main_a", "products_delta"}, nil, "", 0, 0)

	if err != nil {
		t.Fatalf("CreateDistributed() error: %v", err)
	}
	if receivedDistributed != "products" {
		t.Errorf("received distributed = %q, want 'products'", receivedDistributed)
	}
	if len(receivedLocals) != 2 {
		t.Fatalf("received %d locals, want 2", len(receivedLocals))
	}
}

func TestAgentClient_StartImport(t *testing.T) {
	var receivedTable, receivedPath, receivedMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/import" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Errorf("unexpected method: %s", r.Method)
		}

		var body types.ImportRequest
		json.NewDecoder(r.Body).Decode(&body)
		receivedTable = body.Table
		receivedPath = body.CSVPath
		receivedMethod = string(body.Method)

		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"jobId":"test-job-123"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	jobID, err := client.StartImport(types.ImportRequest{
		Table:   "products",
		CSVPath: "/data/products.csv",
		Cluster: "test-cluster",
		Method:  types.ImportMethodIndexer,
	}, 0)

	if err != nil {
		t.Fatalf("StartImport() error: %v", err)
	}
	if jobID != "test-job-123" {
		t.Errorf("jobID = %q, want 'test-job-123'", jobID)
	}
	if receivedTable != "products" {
		t.Errorf("received table = %q, want 'products'", receivedTable)
	}
	if receivedPath != "/data/products.csv" {
		t.Errorf("received csvPath = %q, want '/data/products.csv'", receivedPath)
	}
	if receivedMethod != "indexer" {
		t.Errorf("received method = %q, want 'indexer'", receivedMethod)
	}
}

func TestAgentClient_GetImportStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/import/test-job-123" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != "GET" {
			t.Errorf("unexpected method: %s", r.Method)
		}

		w.Write([]byte(`{"job":{"id":"test-job-123","table":"products","csvPath":"/data/products.csv","status":"completed"}}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	job, err := client.GetImportStatus("test-job-123", 0)

	if err != nil {
		t.Fatalf("GetImportStatus() error: %v", err)
	}
	if job.ID != "test-job-123" {
		t.Errorf("job.ID = %q, want 'test-job-123'", job.ID)
	}
	if job.Status != "completed" {
		t.Errorf("job.Status = %q, want 'completed'", job.Status)
	}
}

func TestAgentClient_Import(t *testing.T) {
	pollCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/import" && r.Method == "POST":
			// Start import - return job ID
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(`{"jobId":"test-job-456"}`))
		case r.URL.Path == "/api/import/test-job-456" && r.Method == "GET":
			// Poll for status
			pollCount++
			if pollCount >= 2 {
				// Return completed on second poll
				w.Write([]byte(`{"job":{"id":"test-job-456","status":"completed"}}`))
			} else {
				// Return running on first poll
				w.Write([]byte(`{"job":{"id":"test-job-456","status":"running"}}`))
			}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	// Use a short poll interval for testing
	config := ImportConfig{
		PollInterval: 10 * time.Millisecond,
		PollTimeout:  5 * time.Second,
	}
	err := client.ImportWithConfig("products", "/data/products.csv", "test-cluster", 0, config)

	if err != nil {
		t.Fatalf("Import() error: %v", err)
	}
	if pollCount < 2 {
		t.Errorf("pollCount = %d, want at least 2", pollCount)
	}
}

func TestAgentClient_Import_Failure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/import" && r.Method == "POST":
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(`{"jobId":"test-job-fail"}`))
		case r.URL.Path == "/api/import/test-job-fail" && r.Method == "GET":
			w.Write([]byte(`{"job":{"id":"test-job-fail","status":"failed","error":"csv file not found"}}`))
		}
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	config := ImportConfig{
		PollInterval: 10 * time.Millisecond,
		PollTimeout:  5 * time.Second,
	}
	err := client.ImportWithConfig("products", "/data/products.csv", "test-cluster", 0, config)

	if err == nil {
		t.Fatal("Import() should have returned error for failed job")
	}
	if !strings.Contains(err.Error(), "csv file not found") {
		t.Errorf("error = %q, should contain 'csv file not found'", err.Error())
	}
}

func TestAgentClient_4xxErrorNoRetry(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	_, err := client.Health(0)

	if err == nil {
		t.Error("Health() should return error for 400")
	}
	if callCount != 1 {
		t.Errorf("4xx errors should not retry, but called %d times", callCount)
	}
}

func TestAgentClient_5xxErrorRetries(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"server error"}`))
			return
		}
		// Succeed on third attempt
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(types.HealthResponse{Status: "ok"})
	}))
	defer server.Close()

	client := NewAgentClient(server.URL, "token")
	resp, err := client.Health(0)

	if err != nil {
		t.Fatalf("Health() error after retries: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want 'ok'", resp.Status)
	}
	if callCount != 3 {
		t.Errorf("Expected 3 calls (2 retries + success), got %d", callCount)
	}
}

func TestResponse_ZeroValue(t *testing.T) {
	resp := types.Response{}
	if resp.Status != "" || resp.Message != "" || resp.Error != "" {
		t.Error("Zero Response should have empty fields")
	}
}

func TestHealthResponse_ZeroValue(t *testing.T) {
	resp := types.HealthResponse{}
	if resp.ClusterStatus != "" || resp.Status != "" {
		t.Error("Zero HealthResponse should have empty fields")
	}
}

func TestTableInfo_ZeroValue(t *testing.T) {
	info := types.TableInfo{}
	if info.Name != "" || info.Type != "" || info.InCluster {
		t.Error("Zero TableInfo should have empty/false fields")
	}
}
