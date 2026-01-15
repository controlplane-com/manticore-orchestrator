package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/agent/manticore"
	"github.com/controlplane-com/manticore-orchestrator/shared/types"
)

func TestParseGrastate(t *testing.T) {
	tests := []struct {
		name          string
		content       string
		expectedUUID  string
		expectedSeqno int64
		expectedSafe  int
		expectError   bool
	}{
		{
			name: "valid grastate with all fields",
			content: `# GALERA saved state
version: 2.1
uuid:    abc123-def456-789
seqno:   42
safe_to_bootstrap: 1
`,
			expectedUUID:  "abc123-def456-789",
			expectedSeqno: 42,
			expectedSafe:  1,
		},
		{
			name: "grastate with safe_to_bootstrap=0",
			content: `uuid: cluster-uuid-here
seqno: 100
safe_to_bootstrap: 0
`,
			expectedUUID:  "cluster-uuid-here",
			expectedSeqno: 100,
			expectedSafe:  0,
		},
		{
			name: "grastate with negative seqno",
			content: `uuid: some-uuid
seqno: -1
safe_to_bootstrap: 0
`,
			expectedUUID:  "some-uuid",
			expectedSeqno: -1,
			expectedSafe:  0,
		},
		{
			name: "grastate with only uuid",
			content: `uuid: only-uuid
`,
			expectedUUID:  "only-uuid",
			expectedSeqno: 0,
			expectedSafe:  0,
		},
		{
			name: "grastate with comments and empty lines",
			content: `# This is a comment
uuid: test-uuid

# Another comment
seqno: 50

safe_to_bootstrap: 1
`,
			expectedUUID:  "test-uuid",
			expectedSeqno: 50,
			expectedSafe:  1,
		},
		{
			name:          "empty file",
			content:       "",
			expectedUUID:  "",
			expectedSeqno: 0,
			expectedSafe:  0,
		},
		{
			name: "grastate with extra whitespace",
			content: `uuid:     spaced-uuid
seqno:   123
safe_to_bootstrap:  1
`,
			expectedUUID:  "spaced-uuid",
			expectedSeqno: 123,
			expectedSafe:  1,
		},
		{
			name: "grastate with invalid seqno (non-numeric)",
			content: `uuid: valid-uuid
seqno: not-a-number
`,
			expectedUUID:  "valid-uuid",
			expectedSeqno: 0, // parsing fails, default to 0
			expectedSafe:  0,
		},
		{
			name: "grastate with malformed lines",
			content: `uuid: good-uuid
this line has no colon
seqno: 99
also no colon here
`,
			expectedUUID:  "good-uuid",
			expectedSeqno: 99,
			expectedSafe:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "grastate.dat")
			if err := os.WriteFile(tmpFile, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			info, err := parseGrastate(tmpFile)

			if tt.expectError {
				if err == nil {
					t.Error("parseGrastate() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("parseGrastate() unexpected error: %v", err)
			}

			if info.UUID != tt.expectedUUID {
				t.Errorf("UUID = %q, want %q", info.UUID, tt.expectedUUID)
			}
			if info.Seqno != tt.expectedSeqno {
				t.Errorf("Seqno = %d, want %d", info.Seqno, tt.expectedSeqno)
			}
			if info.SafeToBootstrap != tt.expectedSafe {
				t.Errorf("SafeToBootstrap = %d, want %d", info.SafeToBootstrap, tt.expectedSafe)
			}
		})
	}
}

func TestParseGrastate_FileNotFound(t *testing.T) {
	_, err := parseGrastate("/nonexistent/path/grastate.dat")
	if err == nil {
		t.Error("parseGrastate() expected error for nonexistent file")
	}
	if !os.IsNotExist(err) {
		t.Errorf("parseGrastate() error should be IsNotExist, got: %v", err)
	}
}

func TestGrastateInfo_ZeroValues(t *testing.T) {
	info := GrastateInfo{}

	if info.UUID != "" {
		t.Errorf("Zero UUID should be empty, got %q", info.UUID)
	}
	if info.Seqno != 0 {
		t.Errorf("Zero Seqno should be 0, got %d", info.Seqno)
	}
	if info.SafeToBootstrap != 0 {
		t.Errorf("Zero SafeToBootstrap should be 0, got %d", info.SafeToBootstrap)
	}
}

func TestInitialized(t *testing.T) {
	// Reset to known state
	SetInitialized(false)

	if Initialized() {
		t.Error("Initialized() should return false after SetInitialized(false)")
	}

	SetInitialized(true)
	if !Initialized() {
		t.Error("Initialized() should return true after SetInitialized(true)")
	}

	// Reset for other tests
	SetInitialized(false)
}

func TestNewHandler(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	// We can't create a real Client without a database, but we can test the constructor logic
	// by checking it doesn't panic and returns non-nil
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	if h == nil {
		t.Fatal("NewHandler() returned nil")
	}
	if h.clusterName != "test-cluster" {
		t.Errorf("clusterName = %q, want 'test-cluster'", h.clusterName)
	}
	if h.s3Mount != "/mnt/s3" {
		t.Errorf("s3Mount = %q, want '/mnt/s3'", h.s3Mount)
	}
	if h.batchSize != 1000 {
		t.Errorf("batchSize = %d, want 1000", h.batchSize)
	}
	if h.importWorkers != 4 {
		t.Errorf("importWorkers = %d, want 4", h.importWorkers)
	}
}

// Response helper tests
func TestJsonResponse(t *testing.T) {
	w := httptest.NewRecorder()
	data := map[string]string{"key": "value"}

	jsonResponse(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want 'application/json'", ct)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("response key = %q, want 'value'", result["key"])
	}
}

func TestErrorResponse(t *testing.T) {
	w := httptest.NewRecorder()

	errorResponse(w, http.StatusBadRequest, "test error")

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if result["error"] != "test error" {
		t.Errorf("error message = %q, want 'test error'", result["error"])
	}
}

func TestSuccessResponse(t *testing.T) {
	w := httptest.NewRecorder()

	successResponse(w, "operation completed")

	if w.Code != http.StatusOK {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("status = %q, want 'ok'", result["status"])
	}
	if result["message"] != "operation completed" {
		t.Errorf("message = %q, want 'operation completed'", result["message"])
	}
}

// Request body validation tests
func TestCreateTableRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/table/create", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.CreateTableHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var result map[string]string
	json.Unmarshal(w.Body.Bytes(), &result)
	if result["error"] != "invalid request body" {
		t.Errorf("error = %q, want 'invalid request body'", result["error"])
	}
}

func TestCreateTableRequest_EmptyTable(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"table": ""}`
	req := httptest.NewRequest("POST", "/api/table/create", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.CreateTableHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var result map[string]string
	json.Unmarshal(w.Body.Bytes(), &result)
	if result["error"] != "table name is required" {
		t.Errorf("error = %q, want 'table name is required'", result["error"])
	}
}

func TestCreateTableRequest_NoSchema(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"table": "unknown_table"}`
	req := httptest.NewRequest("POST", "/api/table/create", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.CreateTableHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var result map[string]string
	json.Unmarshal(w.Body.Bytes(), &result)
	if !bytes.Contains(w.Body.Bytes(), []byte("no schema found")) {
		t.Errorf("error should contain 'no schema found', got %q", result["error"])
	}
}

func TestDropTableRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/table/drop", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.DropTableHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestDropTableRequest_EmptyTable(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"table": ""}`
	req := httptest.NewRequest("POST", "/api/table/drop", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.DropTableHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestClusterJoinRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/cluster/join", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.ClusterJoinHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestClusterJoinRequest_EmptySourceAddr(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"sourceAddr": ""}`
	req := httptest.NewRequest("POST", "/api/cluster/join", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.ClusterJoinHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var result map[string]string
	json.Unmarshal(w.Body.Bytes(), &result)
	if result["error"] != "sourceAddr is required" {
		t.Errorf("error = %q, want 'sourceAddr is required'", result["error"])
	}
}

func TestClusterRejoinRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/cluster/rejoin", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.ClusterRejoinHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestClusterRejoinRequest_EmptySourceAddr(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"sourceAddr": ""}`
	req := httptest.NewRequest("POST", "/api/cluster/rejoin", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.ClusterRejoinHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestAlterDistributedRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/distributed/alter", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.AlterDistributed(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestAlterDistributedRequest_MissingFields(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	tests := []struct {
		name string
		body string
	}{
		{"empty distributed", `{"distributed": "", "locals": ["table1"]}`},
		{"empty locals", `{"distributed": "dist_table", "locals": []}`},
		{"missing locals", `{"distributed": "dist_table"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/distributed/alter", bytes.NewBufferString(tt.body))
			w := httptest.NewRecorder()

			h.AlterDistributed(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestCreateDistributedRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/distributed/create", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.CreateDistributedHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestCreateDistributedRequest_MissingFields(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	tests := []struct {
		name string
		body string
	}{
		{"empty distributed", `{"distributed": "", "locals": ["table1"]}`},
		{"empty locals", `{"distributed": "dist_table", "locals": []}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/distributed/create", bytes.NewBufferString(tt.body))
			w := httptest.NewRecorder()

			h.CreateDistributedHandler(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestClusterAddRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/cluster/add", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.ClusterAddHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestClusterAddRequest_EmptyTable(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"table": ""}`
	req := httptest.NewRequest("POST", "/api/cluster/add", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.ClusterAddHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestClusterDropRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/cluster/drop", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.ClusterDrop(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestClusterDropRequest_EmptyTable(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"table": ""}`
	req := httptest.NewRequest("POST", "/api/cluster/drop", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.ClusterDrop(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestImportRequest_InvalidBody(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	req := httptest.NewRequest("POST", "/api/import", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.StartImport(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestImportRequest_MissingFields(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	tests := []struct {
		name string
		body string
	}{
		{"empty table", `{"table": "", "csvPath": "/path/to/file.csv"}`},
		{"empty csvPath", `{"table": "products", "csvPath": ""}`},
		{"missing both", `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/import", bytes.NewBufferString(tt.body))
			w := httptest.NewRecorder()

			h.StartImport(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestImportRequest_NoSchema(t *testing.T) {
	registry := manticore.NewSchemaRegistry()
	h := NewHandler(nil, registry, "test-cluster", "/mnt/s3", 1000, 4, nil)

	body := `{"table": "unknown_table", "csvPath": "/path/to/file.csv"}`
	req := httptest.NewRequest("POST", "/api/import", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.StartImport(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code = %d, want %d", w.Code, http.StatusBadRequest)
	}

	if !bytes.Contains(w.Body.Bytes(), []byte("no schema found")) {
		t.Errorf("error should contain 'no schema found', got %s", w.Body.String())
	}
}

// GrastateResponse struct test
func TestGrastateResponse_ZeroValue(t *testing.T) {
	resp := types.GrastateResponse{}
	if resp.UUID != "" || resp.Seqno != 0 || resp.SafeToBootstrap != 0 || resp.Exists {
		t.Error("Zero GrastateResponse should have zero values")
	}
}

// TableInfo struct test
func TestTableInfo_ZeroValue(t *testing.T) {
	resp := types.TableInfo{}
	if resp.Name != "" || resp.Type != "" || resp.InCluster {
		t.Error("Zero TableInfo should have zero values")
	}
}
