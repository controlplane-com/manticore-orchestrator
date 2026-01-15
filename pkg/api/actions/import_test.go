package actions

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/controlplane-com/manticore-orchestrator/api/client"
	"github.com/controlplane-com/manticore-orchestrator/shared/types"
)

// mockImportServer creates a test server that simulates agent responses for import operations
// It handles health checks (for slot discovery) and tracks table state
func mockImportServer(t *testing.T, existingTables []string) *httptest.Server {
	var mu sync.Mutex
	tables := make(map[string]bool)
	for _, name := range existingTables {
		tables[name] = true
	}

	// Helper to discover slot based on which main table exists
	discoverSlot := func(baseName string) string {
		if tables[baseName+"_main_b"] {
			return "b"
		}
		if tables[baseName+"_main_a"] {
			return "a"
		}
		return "a" // Default
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/health":
			// Return healthy cluster member status for slot discovery
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})

		case "/api/table/create":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			tables[req.Table] = true
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/cluster/add":
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/import":
			if r.Method == "POST" {
				// Start async import - return job ID
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte(`{"jobId":"test-job-123"}`))
			}

		case "/api/tables":
			var tableList []types.TableInfo
			// Add base table with slot (simulates agent behavior)
			// The base table name is "products" for these tests
			tableList = append(tableList, types.TableInfo{
				Name: "products",
				Slot: discoverSlot("products"),
			})
			for name := range tables {
				tableList = append(tableList, types.TableInfo{Name: name})
			}
			json.NewEncoder(w).Encode(types.TablesResponse{Tables: tableList})

		case "/api/distributed/alter":
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/cluster/drop":
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/table/drop":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			delete(tables, req.Table)
			w.Write([]byte(`{"status":"ok"}`))

		default:
			// Handle /api/import/{jobId} pattern for polling
			if strings.HasPrefix(r.URL.Path, "/api/import/") && r.Method == "GET" {
				w.Write([]byte(`{"job":{"id":"test-job-123","status":"completed"}}`))
				return
			}
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
}

func TestImport_SlotSwapFromAToB(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Start with products_main_a existing (slot A is current)
	server := mockImportServer(t, []string{"products_main_a", "products_delta"})
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{client.NewAgentClient(server.URL, "token")},
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err != nil {
		t.Fatalf("Import() error: %v", err)
	}

	// Verify by listing tables - should have products_main_b (new) and not products_main_a (dropped)
	tables, err := ctx.Clients[0].ListTables(0)
	if err != nil {
		t.Fatalf("ListTables() error: %v", err)
	}

	hasMainA := false
	hasMainB := false
	for _, table := range tables {
		if table.Name == "products_main_a" {
			hasMainA = true
		}
		if table.Name == "products_main_b" {
			hasMainB = true
		}
	}

	if hasMainA {
		t.Error("products_main_a should have been dropped")
	}
	if !hasMainB {
		t.Error("products_main_b should have been created")
	}
}

func TestImport_SlotSwapFromBToA(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Start with products_main_b existing (slot B is current)
	server := mockImportServer(t, []string{"products_main_b", "products_delta"})
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{client.NewAgentClient(server.URL, "token")},
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err != nil {
		t.Fatalf("Import() error: %v", err)
	}

	// Verify by listing tables - should have products_main_a (new) and not products_main_b (dropped)
	tables, err := ctx.Clients[0].ListTables(0)
	if err != nil {
		t.Fatalf("ListTables() error: %v", err)
	}

	hasMainA := false
	hasMainB := false
	for _, table := range tables {
		if table.Name == "products_main_a" {
			hasMainA = true
		}
		if table.Name == "products_main_b" {
			hasMainB = true
		}
	}

	if !hasMainA {
		t.Error("products_main_a should have been created")
	}
	if hasMainB {
		t.Error("products_main_b should have been dropped")
	}
}

func TestImport_DefaultsToSlotAWhenNoMainTableExists(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Start with no main tables - should detect default slot "a" and create "b"
	server := mockImportServer(t, []string{"products_delta"})
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{client.NewAgentClient(server.URL, "token")},
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err != nil {
		t.Fatalf("Import() error: %v", err)
	}

	// Should create products_main_b (swapped from default "a")
	tables, err := ctx.Clients[0].ListTables(0)
	if err != nil {
		t.Fatalf("ListTables() error: %v", err)
	}

	hasMainB := false
	for _, table := range tables {
		if table.Name == "products_main_b" {
			hasMainB = true
		}
	}

	if !hasMainB {
		t.Error("products_main_b should have been created (swapped from default 'a')")
	}
}

func TestImport_NoClients(t *testing.T) {
	ctx := &Context{
		Clients: []*client.AgentClient{},
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err == nil {
		t.Error("Import() should return error when no clients configured")
	}
}

func TestImport_MultipleReplicas(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Track which replicas received AlterDistributed calls
	var alterCalls []int
	var mu sync.Mutex
	tables := map[string]bool{"products_main_a": true, "products_delta": true}

	// Helper to discover slot based on which main table exists
	discoverSlot := func(baseName string) string {
		if tables[baseName+"_main_b"] {
			return "b"
		}
		if tables[baseName+"_main_a"] {
			return "a"
		}
		return "a"
	}

	servers := make([]*httptest.Server, 3)
	for i := 0; i < 3; i++ {
		replicaIndex := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			switch r.URL.Path {
			case "/api/health":
				json.NewEncoder(w).Encode(types.HealthResponse{
					Status:        "ok",
					ClusterStatus: "primary",
					NodeState:     "synced",
				})
			case "/api/distributed/alter":
				mu.Lock()
				alterCalls = append(alterCalls, replicaIndex)
				mu.Unlock()
				w.Write([]byte(`{"status":"ok"}`))
			case "/api/tables":
				mu.Lock()
				var tableList []types.TableInfo
				// Add base table with slot
				tableList = append(tableList, types.TableInfo{
					Name: "products",
					Slot: discoverSlot("products"),
				})
				for name := range tables {
					tableList = append(tableList, types.TableInfo{Name: name})
				}
				mu.Unlock()
				json.NewEncoder(w).Encode(types.TablesResponse{Tables: tableList})
			case "/api/table/create":
				var req struct {
					Table string `json:"table"`
				}
				json.NewDecoder(r.Body).Decode(&req)
				mu.Lock()
				tables[req.Table] = true
				mu.Unlock()
				w.Write([]byte(`{"status":"ok"}`))
			case "/api/import":
				// Start async import - return job ID
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte(`{"jobId":"test-job-multi"}`))
			default:
				// Handle /api/import/{jobId} pattern for polling
				if strings.HasPrefix(r.URL.Path, "/api/import/") && r.Method == "GET" {
					w.Write([]byte(`{"job":{"id":"test-job-multi","status":"completed"}}`))
					return
				}
				w.Write([]byte(`{"status":"ok"}`))
			}
		}))
		defer servers[i].Close()
	}

	clients := make([]*client.AgentClient, 3)
	for i := 0; i < 3; i++ {
		clients[i] = client.NewAgentClient(servers[i].URL, "token")
	}

	ctx := &Context{
		Clients: clients,
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err != nil {
		t.Fatalf("Import() error: %v", err)
	}

	// Verify all 3 replicas received AlterDistributed calls
	if len(alterCalls) != 3 {
		t.Errorf("AlterDistributed called %d times, want 3", len(alterCalls))
	}
}

func TestImport_MainTableNames(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Test that correct table names are used based on discovered slot
	var createTableCalls []string
	var mu sync.Mutex
	tables := map[string]bool{"mydata_main_a": true} // Start with slot A

	// Helper to discover slot based on which main table exists
	discoverSlot := func(baseName string) string {
		if tables[baseName+"_main_b"] {
			return "b"
		}
		if tables[baseName+"_main_a"] {
			return "a"
		}
		return "a"
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})
		case "/api/table/create":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			createTableCalls = append(createTableCalls, req.Table)
			tables[req.Table] = true
			mu.Unlock()
			w.Write([]byte(`{"status":"ok"}`))
		case "/api/tables":
			mu.Lock()
			var tableList []types.TableInfo
			// Add base table with slot
			tableList = append(tableList, types.TableInfo{
				Name: "mydata",
				Slot: discoverSlot("mydata"),
			})
			for name := range tables {
				tableList = append(tableList, types.TableInfo{Name: name})
			}
			mu.Unlock()
			json.NewEncoder(w).Encode(types.TablesResponse{Tables: tableList})
		case "/api/import":
			// Start async import - return job ID
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(`{"jobId":"test-job-main"}`))
		default:
			// Handle /api/import/{jobId} pattern for polling
			if strings.HasPrefix(r.URL.Path, "/api/import/") && r.Method == "GET" {
				w.Write([]byte(`{"job":{"id":"test-job-main","status":"completed"}}`))
				return
			}
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{client.NewAgentClient(server.URL, "token")},
		Dataset: "mydata",
		CSVPath: "/data/mydata.csv",
	}

	err := Import(context.Background(), ctx)
	if err != nil {
		t.Fatalf("Import() error: %v", err)
	}

	// Since we're on slot a, import should create slot b table
	found := false
	for _, name := range createTableCalls {
		if name == "mydata_main_b" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected mydata_main_b to be created, got calls: %v", createTableCalls)
	}
}

func TestWaitForReplication_AllReplicasReady(t *testing.T) {
	servers := make([]*httptest.Server, 2)
	for i := 0; i < 2; i++ {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/api/tables" {
				json.NewEncoder(w).Encode(types.TablesResponse{
					Tables: []types.TableInfo{{Name: "test_table"}},
				})
			}
		}))
		defer servers[i].Close()
	}

	clients := make([]*client.AgentClient, 2)
	for i := 0; i < 2; i++ {
		clients[i] = client.NewAgentClient(servers[i].URL, "token")
	}

	ctx := &Context{Clients: clients}
	err := waitForReplication(context.Background(), ctx, "test_table")
	if err != nil {
		t.Errorf("waitForReplication() error: %v", err)
	}
}

func TestWaitForReplication_ContextCancellation(t *testing.T) {
	// Server that never returns the table (would timeout without cancellation)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/tables" {
			// Return empty tables list - table never replicates
			json.NewEncoder(w).Encode(types.TablesResponse{Tables: []types.TableInfo{}})
		}
	}))
	defer server.Close()

	clients := []*client.AgentClient{client.NewAgentClient(server.URL, "token")}
	ctx := &Context{Clients: clients}

	// Create a context that we'll cancel immediately
	goCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := waitForReplication(goCtx, ctx, "test_table")
	if err == nil {
		t.Error("waitForReplication() should return error when context is cancelled")
	}
	if err != context.Canceled {
		t.Errorf("waitForReplication() error = %v, want context.Canceled", err)
	}
}

func TestImport_CleanupOnImportFailure(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Track cleanup operations
	var mu sync.Mutex
	tables := map[string]bool{"products_main_a": true, "products_delta": true}
	var clusterDropCalls []string
	var tableDropCalls []string

	// Helper to discover slot based on which main table exists
	discoverSlot := func(baseName string) string {
		if tables[baseName+"_main_b"] {
			return "b"
		}
		if tables[baseName+"_main_a"] {
			return "a"
		}
		return "a"
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})

		case "/api/table/create":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			tables[req.Table] = true
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/cluster/add":
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/cluster/drop":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			clusterDropCalls = append(clusterDropCalls, req.Table)
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/table/drop":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			tableDropCalls = append(tableDropCalls, req.Table)
			delete(tables, req.Table)
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/import":
			if r.Method == "POST" {
				// Start async import - return job ID
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte(`{"jobId":"test-job-fail"}`))
			}

		case "/api/tables":
			var tableList []types.TableInfo
			// Add base table with slot (required for slot discovery)
			tableList = append(tableList, types.TableInfo{
				Name: "products",
				Slot: discoverSlot("products"),
			})
			for name := range tables {
				tableList = append(tableList, types.TableInfo{Name: name})
			}
			json.NewEncoder(w).Encode(types.TablesResponse{Tables: tableList})

		default:
			// Handle /api/import/{jobId} pattern - return FAILED status
			if strings.HasPrefix(r.URL.Path, "/api/import/") && r.Method == "GET" {
				w.Write([]byte(`{"job":{"id":"test-job-fail","status":"failed","error":"simulated import failure"}}`))
				return
			}
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{client.NewAgentClient(server.URL, "token")},
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err == nil {
		t.Fatal("Import() should return error when import fails")
	}

	// Verify cleanup was called - ClusterDrop should be called for the new table
	if len(clusterDropCalls) == 0 {
		t.Error("ClusterDrop should have been called during cleanup")
	} else {
		found := false
		for _, table := range clusterDropCalls {
			if table == "products_main_b" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ClusterDrop should have been called for products_main_b, got: %v", clusterDropCalls)
		}
	}

	// Verify table was dropped
	if len(tableDropCalls) == 0 {
		t.Error("DropTable should have been called during cleanup")
	} else {
		found := false
		for _, table := range tableDropCalls {
			if table == "products_main_b" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("DropTable should have been called for products_main_b, got: %v", tableDropCalls)
		}
	}
}

func TestImport_CleanupOnContextCancellation(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Track cleanup operations
	var mu sync.Mutex
	tables := map[string]bool{"products_main_a": true, "products_delta": true}
	var clusterDropCalls []string
	var tableDropCalls []string
	var importCancelCalls []string
	importStarted := make(chan struct{})

	// Helper to discover slot based on which main table exists
	discoverSlot := func(baseName string) string {
		if tables[baseName+"_main_b"] {
			return "b"
		}
		if tables[baseName+"_main_a"] {
			return "a"
		}
		return "a"
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})

		case "/api/table/create":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			tables[req.Table] = true
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/cluster/add":
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/cluster/drop":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			clusterDropCalls = append(clusterDropCalls, req.Table)
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/table/drop":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			tableDropCalls = append(tableDropCalls, req.Table)
			delete(tables, req.Table)
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/import":
			if r.Method == "POST" {
				// Start async import - return job ID
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte(`{"jobId":"test-job-cancel"}`))
				// Signal that import has started
				select {
				case importStarted <- struct{}{}:
				default:
				}
			}

		case "/api/tables":
			var tableList []types.TableInfo
			// Add base table with slot (required for slot discovery)
			tableList = append(tableList, types.TableInfo{
				Name: "products",
				Slot: discoverSlot("products"),
			})
			for name := range tables {
				tableList = append(tableList, types.TableInfo{Name: name})
			}
			json.NewEncoder(w).Encode(types.TablesResponse{Tables: tableList})

		default:
			// Handle DELETE /api/import/{jobId} - cancel import
			if strings.HasPrefix(r.URL.Path, "/api/import/") && r.Method == "DELETE" {
				jobID := strings.TrimPrefix(r.URL.Path, "/api/import/")
				importCancelCalls = append(importCancelCalls, jobID)
				w.Write([]byte(`{"status":"ok"}`))
				return
			}
			// Handle GET /api/import/{jobId} - return running status
			if strings.HasPrefix(r.URL.Path, "/api/import/") && r.Method == "GET" {
				w.Write([]byte(`{"job":{"id":"test-job-cancel","status":"running"}}`))
				return
			}
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{client.NewAgentClient(server.URL, "token")},
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	// Create a cancellable context
	goCtx, cancel := context.WithCancel(context.Background())

	// Run import in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- Import(goCtx, ctx)
	}()

	// Wait for import to start, then cancel
	<-importStarted
	cancel()

	// Wait for import to finish
	err := <-errCh
	if err == nil {
		t.Fatal("Import() should return error when context is cancelled")
	}

	// Verify import was cancelled on the agent
	mu.Lock()
	cancelCalls := len(importCancelCalls)
	mu.Unlock()
	if cancelCalls == 0 {
		t.Error("CancelImport should have been called on the agent")
	}

	// Verify cleanup was called
	mu.Lock()
	clusterDropCount := len(clusterDropCalls)
	tableDropCount := len(tableDropCalls)
	mu.Unlock()

	if clusterDropCount == 0 {
		t.Error("ClusterDrop should have been called during cleanup")
	}
	if tableDropCount == 0 {
		t.Error("DropTable should have been called during cleanup")
	}
}

func TestImport_NoCleanupBeforeClusterAdd(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Track cleanup operations
	var mu sync.Mutex
	var clusterDropCalls []string
	var tableDropCalls []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(types.HealthResponse{
				Status:        "ok",
				ClusterStatus: "primary",
				NodeState:     "synced",
			})

		case "/api/table/create":
			// Fail table creation - before ClusterAdd
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"simulated create failure"}`))

		case "/api/cluster/drop":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			clusterDropCalls = append(clusterDropCalls, req.Table)
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/table/drop":
			var req struct {
				Table string `json:"table"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			tableDropCalls = append(tableDropCalls, req.Table)
			w.Write([]byte(`{"status":"ok"}`))

		case "/api/tables":
			json.NewEncoder(w).Encode(types.TablesResponse{
				Tables: []types.TableInfo{{Name: "products_main_a"}},
			})

		default:
			w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer server.Close()

	ctx := &Context{
		Clients: []*client.AgentClient{client.NewAgentClient(server.URL, "token")},
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err == nil {
		t.Fatal("Import() should return error when create table fails")
	}

	// Verify NO cleanup was called (failure was before ClusterAdd)
	mu.Lock()
	clusterDropCount := len(clusterDropCalls)
	tableDropCount := len(tableDropCalls)
	mu.Unlock()

	if clusterDropCount > 0 {
		t.Errorf("ClusterDrop should NOT have been called before ClusterAdd succeeds, got: %v", clusterDropCalls)
	}
	if tableDropCount > 0 {
		t.Errorf("DropTable should NOT have been called before ClusterAdd succeeds, got: %v", tableDropCalls)
	}
}

func TestImport_CleanupOnMultipleReplicas(t *testing.T) {
	// Use short poll interval to speed up test
	t.Setenv("IMPORT_POLL_INTERVAL", "100ms")

	// Track cleanup operations across multiple replicas
	var mu sync.Mutex
	tables := map[string]bool{"products_main_a": true, "products_delta": true}
	tableDropCalls := make(map[int][]string) // replica index -> tables dropped

	// Helper to discover slot based on which main table exists
	discoverSlot := func(baseName string) string {
		if tables[baseName+"_main_b"] {
			return "b"
		}
		if tables[baseName+"_main_a"] {
			return "a"
		}
		return "a"
	}

	servers := make([]*httptest.Server, 3)
	for i := 0; i < 3; i++ {
		replicaIndex := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()

			w.Header().Set("Content-Type", "application/json")

			switch r.URL.Path {
			case "/api/health":
				json.NewEncoder(w).Encode(types.HealthResponse{
					Status:        "ok",
					ClusterStatus: "primary",
					NodeState:     "synced",
				})

			case "/api/table/create":
				var req struct {
					Table string `json:"table"`
				}
				json.NewDecoder(r.Body).Decode(&req)
				tables[req.Table] = true
				w.Write([]byte(`{"status":"ok"}`))

			case "/api/cluster/add", "/api/cluster/drop":
				w.Write([]byte(`{"status":"ok"}`))

			case "/api/table/drop":
				var req struct {
					Table string `json:"table"`
				}
				json.NewDecoder(r.Body).Decode(&req)
				tableDropCalls[replicaIndex] = append(tableDropCalls[replicaIndex], req.Table)
				delete(tables, req.Table)
				w.Write([]byte(`{"status":"ok"}`))

			case "/api/import":
				if r.Method == "POST" {
					w.WriteHeader(http.StatusAccepted)
					w.Write([]byte(`{"jobId":"test-job-multi-cleanup"}`))
				}

			case "/api/tables":
				var tableList []types.TableInfo
				// Add base table with slot (required for slot discovery)
				tableList = append(tableList, types.TableInfo{
					Name: "products",
					Slot: discoverSlot("products"),
				})
				for name := range tables {
					tableList = append(tableList, types.TableInfo{Name: name})
				}
				json.NewEncoder(w).Encode(types.TablesResponse{Tables: tableList})

			default:
				// Return failed status for import polling
				if strings.HasPrefix(r.URL.Path, "/api/import/") && r.Method == "GET" {
					w.Write([]byte(`{"job":{"id":"test-job-multi-cleanup","status":"failed","error":"simulated failure"}}`))
					return
				}
				w.Write([]byte(`{"status":"ok"}`))
			}
		}))
		defer servers[i].Close()
	}

	clients := make([]*client.AgentClient, 3)
	for i := 0; i < 3; i++ {
		clients[i] = client.NewAgentClient(servers[i].URL, "token")
	}

	ctx := &Context{
		Clients: clients,
		Dataset: "products",
		CSVPath: "/data/products.csv",
	}

	err := Import(context.Background(), ctx)
	if err == nil {
		t.Fatal("Import() should return error when import fails")
	}

	// Verify cleanup was called on ALL replicas
	mu.Lock()
	defer mu.Unlock()

	for i := 0; i < 3; i++ {
		drops := tableDropCalls[i]
		found := false
		for _, table := range drops {
			if table == "products_main_b" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Replica %d should have dropped products_main_b, got: %v", i, drops)
		}
	}
}
