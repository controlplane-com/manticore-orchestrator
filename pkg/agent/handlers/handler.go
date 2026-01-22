package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/controlplane-com/manticore-orchestrator/pkg/agent/jobs"
	"github.com/controlplane-com/manticore-orchestrator/pkg/agent/manticore"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/schema"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
	"github.com/gorilla/mux"
)

type Handler struct {
	client        *manticore.Client
	registry      *manticore.SchemaRegistry
	clusterName   string
	s3Mount       string
	batchSize     int           // Batch size for csv-to-manticore imports
	importWorkers int           // Default number of import worker goroutines
	jobManager    *jobs.Manager // Job manager for async imports
}

func NewHandler(client *manticore.Client, registry *manticore.SchemaRegistry, clusterName, s3Mount string, batchSize, importWorkers int, jobManager *jobs.Manager) *Handler {
	return &Handler{
		client:        client,
		registry:      registry,
		clusterName:   clusterName,
		s3Mount:       s3Mount,
		batchSize:     batchSize,
		importWorkers: importWorkers,
		jobManager:    jobManager,
	}
}

// Response helpers
func jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func errorResponse(w http.ResponseWriter, status int, message string) {
	jsonResponse(w, status, map[string]string{"error": message})
}

func successResponse(w http.ResponseWriter, message string) {
	jsonResponse(w, http.StatusOK, map[string]string{"status": "ok", "message": message})
}

// Health returns searchd and cluster status
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	status, err := h.client.GetStatus()
	if err != nil {
		errorResponse(w, http.StatusServiceUnavailable, fmt.Sprintf("searchd not ready: %v", err))
		return
	}

	clusterStatus, _ := h.client.GetClusterStatus(h.clusterName)
	nodeState, _ := h.client.GetClusterNodeState(h.clusterName)

	response := map[string]interface{}{
		"status":        "ok",
		"searchd":       status,
		"cluster":       h.clusterName,
		"clusterStatus": clusterStatus,
		"nodeState":     nodeState,
	}

	jsonResponse(w, http.StatusOK, response)
}

// GrastateInfo holds parsed grastate.dat information
type GrastateInfo struct {
	UUID            string
	Seqno           int64
	SafeToBootstrap int
}

// parseGrastate reads and parses the grastate.dat file
func parseGrastate(path string) (*GrastateInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info := &GrastateInfo{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "uuid":
			info.UUID = value
		case "seqno":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				info.Seqno = v
			}
		case "safe_to_bootstrap":
			if v, err := strconv.Atoi(value); err == nil {
				info.SafeToBootstrap = v
			}
		}
	}

	return info, scanner.Err()
}

// Grastate returns the grastate.dat information for cluster repair decisions
func (h *Handler) Grastate(w http.ResponseWriter, r *http.Request) {
	grastatePath := "/var/lib/manticore/grastate.dat"

	// Debug: check if path exists and list directory
	if _, statErr := os.Stat(grastatePath); statErr != nil {
		slog.Debug("grastate stat error", "error", statErr)
		// Try to list the directory to see what's there
		entries, dirErr := os.ReadDir("/var/lib/manticore")
		if dirErr != nil {
			slog.Debug("cannot read /var/lib/manticore", "error", dirErr)
		} else {
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			slog.Debug("/var/lib/manticore contents", "files", names)
		}
	}

	info, err := parseGrastate(grastatePath)
	if err != nil {
		slog.Debug("parseGrastate error", "error", err, "isNotExist", os.IsNotExist(err))
		if os.IsNotExist(err) {
			jsonResponse(w, http.StatusOK, types.GrastateResponse{Exists: false})
			return
		}
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to parse grastate.dat: %v", err))
		return
	}
	slog.Debug("grastate request", "uuid", info.UUID, "exists", true)
	jsonResponse(w, http.StatusOK, types.GrastateResponse{
		UUID:            info.UUID,
		Seqno:           info.Seqno,
		SafeToBootstrap: info.SafeToBootstrap,
		Exists:          true,
	})
}

// Ready is a readiness probe endpoint that checks if the node is ready to serve traffic.
// Ready conditions:
//   - MySQL port is responding
//   - Either no cluster configured (fresh start, waiting for init) OR cluster status is "primary"
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	// Check if MySQL/searchd is responding
	_, err := h.client.GetStatus()
	if err != nil {
		jsonResponse(w, http.StatusServiceUnavailable, map[string]interface{}{
			"ready":  false,
			"reason": fmt.Sprintf("searchd not ready: %v", err),
		})
		return
	}

	if !Initialized() {
		jsonResponse(w, http.StatusServiceUnavailable, map[string]interface{}{
			"ready":  false,
			"reason": "not initialized",
		})
		return
	}

	// Check cluster status
	clusterStatus, err := h.client.GetClusterStatus(h.clusterName)
	if err != nil || clusterStatus == "" || clusterStatus == "unknown" {
		//No cluster at all.
		jsonResponse(w, http.StatusServiceUnavailable, map[string]interface{}{
			"ready":         false,
			"reason":        "cluster not synced",
			"clusterStatus": clusterStatus,
		})
		return
	}

	// Cluster exists - check if we're synced (status is "primary")
	if clusterStatus == "primary" {
		jsonResponse(w, http.StatusOK, map[string]interface{}{
			"ready":         true,
			"reason":        "cluster synced",
			"clusterStatus": clusterStatus,
		})
		return
	}

	// In cluster but not synced yet
	jsonResponse(w, http.StatusServiceUnavailable, map[string]interface{}{
		"ready":         false,
		"reason":        "cluster not synced",
		"clusterStatus": clusterStatus,
	})
}

var initialized bool

func Initialized() bool {
	return initialized
}
func SetInitialized(b bool) {
	initialized = b
}

// DiscoverSlot determines the active slot for a base table name.
// Only checks the distributed table to see which main table it references.
// Returns empty string if slot cannot be determined from distributed table.
func (h *Handler) DiscoverSlot(tableName string) string {
	mainA := tableName + "_main_a"
	mainB := tableName + "_main_b"

	// Only check the distributed table - no fallback
	locals, err := h.client.GetDistributedLocals(tableName)
	if err != nil || len(locals) == 0 {
		return "" // No answer - distributed table doesn't exist or is empty
	}

	for _, local := range locals {
		if local == mainA {
			return "a"
		}
		if local == mainB {
			return "b"
		}
	}

	return "" // Distributed table exists but doesn't reference main_a or main_b
}

// GetTables returns all tables (for internal use, no HTTP response)
func (h *Handler) GetTables() ([]types.TableInfo, error) {
	tables, err := h.client.ListTables()
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	result := make([]types.TableInfo, len(tables))
	for i, t := range tables {
		result[i] = types.TableInfo{
			Name: t.Name,
			Type: t.Type,
		}
		// Add slot for base tables (those registered in schema)
		if _, ok := h.registry.Get(t.Name); ok {
			result[i].Slot = h.DiscoverSlot(t.Name)
		}
	}
	return result, nil
}

// ListTables returns all tables with cluster membership info
func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	tables, err := h.client.ListTables()
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to list tables: %v", err))
		return
	}

	// Get cluster tables for membership check
	clusterTables, err := h.client.GetClusterTables(h.clusterName)
	if err != nil {
		slog.Warn("failed to get cluster tables", "error", err)
		clusterTables = make(map[string]bool)
	}

	// Build response with inCluster field and slot for base tables
	response := make([]types.TableInfo, len(tables))
	for i, t := range tables {
		response[i] = types.TableInfo{
			Name:      t.Name,
			Type:      t.Type,
			InCluster: clusterTables[t.Name],
		}
		// Add slot for base tables (those registered in schema)
		if _, ok := h.registry.Get(t.Name); ok {
			response[i].Slot = h.DiscoverSlot(t.Name)
		}
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{"tables": response})
}

// CreateTable creates an RT table with the configured schema (idempotent)
func (h *Handler) CreateTable(tableName string) error {
	// Look up schema by base table name (addresses_main_a -> addresses)
	schema, ok := h.registry.GetForDerivedTable(tableName)
	if !ok {
		baseName := manticore.ExtractBaseTableName(tableName)
		return fmt.Errorf("no schema found for table %s (base: %s)", tableName, baseName)
	}

	// Check if table already exists (idempotent)
	exists, err := h.client.TableExists(tableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if exists {
		slog.Debug("table already exists (idempotent)", "table", tableName)
		return nil
	}

	sql := schema.GenerateCreateTableSQL(tableName)
	slog.Debug("creating table", "table", tableName, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// CreateTableHandler is the HTTP handler for CreateTable
func (h *Handler) CreateTableHandler(w http.ResponseWriter, r *http.Request) {
	var req types.CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Table == "" {
		errorResponse(w, http.StatusBadRequest, "table name is required")
		return
	}

	// Validate schema exists before calling internal method (return 400 for user errors)
	if _, ok := h.registry.GetForDerivedTable(req.Table); !ok {
		baseName := manticore.ExtractBaseTableName(req.Table)
		errorResponse(w, http.StatusBadRequest, fmt.Sprintf("no schema found for table %s (base: %s)", req.Table, baseName))
		return
	}

	if err := h.CreateTable(req.Table); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	jsonResponse(w, http.StatusCreated, map[string]string{"status": "ok", "message": fmt.Sprintf("table %s created", req.Table)})
}

// DropTable drops a table (idempotent - uses IF EXISTS)
func (h *Handler) DropTable(tableName string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	slog.Debug("dropping table", "table", tableName, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	return nil
}

// DropTableHandler is the HTTP handler for DropTable
func (h *Handler) DropTableHandler(w http.ResponseWriter, r *http.Request) {
	var req types.DropTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Table == "" {
		errorResponse(w, http.StatusBadRequest, "table name is required")
		return
	}

	if err := h.DropTable(req.Table); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	successResponse(w, fmt.Sprintf("table %s dropped", req.Table))
}

// AlterDistributed alters a distributed table's local references and agents
func (h *Handler) AlterDistributed(w http.ResponseWriter, r *http.Request) {
	var req types.AlterDistributedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Distributed == "" || len(req.Locals) == 0 {
		errorResponse(w, http.StatusBadRequest, "distributed and locals are required")
		return
	}

	// Build ALTER TABLE statement
	var clauses []string
	for _, local := range req.Locals {
		clauses = append(clauses, fmt.Sprintf("local='%s'", local))
	}

	// Add agent mirrors for EACH local table (both main and delta)
	if len(req.Agents) > 0 {
		mirrorList := strings.Join(req.Agents, "|")
		for _, local := range req.Locals {
			agentClause := fmt.Sprintf("agent='%s:%s'", mirrorList, local)
			clauses = append(clauses, agentClause)
		}

		// Add HA options (only relevant when agents are configured)
		if req.HAStrategy != "" {
			clauses = append(clauses, fmt.Sprintf("ha_strategy='%s'", req.HAStrategy))
		}
		if req.AgentRetryCount > 0 {
			clauses = append(clauses, fmt.Sprintf("agent_retry_count=%d", req.AgentRetryCount))
		}
	}

	sql := fmt.Sprintf("ALTER TABLE %s %s", req.Distributed, strings.Join(clauses, " "))
	slog.Debug("altering distributed table", "table", req.Distributed, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to alter distributed table: %v", err))
		return
	}

	successResponse(w, fmt.Sprintf("distributed table %s altered", req.Distributed))
}

// CreateDistributed creates or updates a distributed table to point to the specified locals and agents
func (h *Handler) CreateDistributed(tableName string, locals []string, agents []string, haStrategy string, agentRetryCount int) error {
	var clauses []string

	// Add local tables
	for _, local := range locals {
		clauses = append(clauses, fmt.Sprintf("local='%s'", local))
	}

	// Add agent mirrors for EACH local table (both main and delta)
	if len(agents) > 0 {
		mirrorList := strings.Join(agents, "|")
		for _, local := range locals {
			agentClause := fmt.Sprintf("agent='%s:%s'", mirrorList, local)
			clauses = append(clauses, agentClause)
		}

		// Add HA options (only relevant when agents are configured)
		if haStrategy != "" {
			clauses = append(clauses, fmt.Sprintf("ha_strategy='%s'", haStrategy))
		}
		if agentRetryCount > 0 {
			clauses = append(clauses, fmt.Sprintf("agent_retry_count=%d", agentRetryCount))
		}
	}

	// Check if table already exists
	exists, err := h.client.TableExists(tableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if exists {
		// Table exists - ALTER to ensure it points to correct locals/agents
		sql := fmt.Sprintf("ALTER TABLE %s %s", tableName, strings.Join(clauses, " "))
		slog.Debug("altering distributed table", "table", tableName, "sql", sql)
		if err := h.client.Execute(sql); err != nil {
			return fmt.Errorf("failed to alter distributed table: %w", err)
		}
		return nil
	}

	// Table doesn't exist - CREATE it
	sql := fmt.Sprintf("CREATE TABLE %s TYPE='distributed' %s", tableName, strings.Join(clauses, " "))
	slog.Debug("creating distributed table", "table", tableName, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		return fmt.Errorf("failed to create distributed table: %w", err)
	}

	return nil
}

// CreateDistributedHandler is the HTTP handler for CreateDistributed
func (h *Handler) CreateDistributedHandler(w http.ResponseWriter, r *http.Request) {
	var req types.CreateDistributedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Distributed == "" || len(req.Locals) == 0 {
		errorResponse(w, http.StatusBadRequest, "distributed and locals are required")
		return
	}

	if err := h.CreateDistributed(req.Distributed, req.Locals, req.Agents, req.HAStrategy, req.AgentRetryCount); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	successResponse(w, fmt.Sprintf("distributed table %s created", req.Distributed))
}

// ClusterAdd adds a table to the cluster for replication (idempotent)
func (h *Handler) ClusterAdd(tableName string) error {
	sql := fmt.Sprintf("ALTER CLUSTER %s ADD %s", h.clusterName, tableName)
	slog.Debug("adding table to cluster", "table", tableName, "cluster", h.clusterName, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		// Check if table is already in cluster (idempotent)
		if strings.Contains(err.Error(), "already part of cluster") || strings.Contains(err.Error(), "already") {
			slog.Debug("table already in cluster (idempotent)", "table", tableName, "cluster", h.clusterName)
			return nil
		}
		return fmt.Errorf("failed to add table to cluster: %w", err)
	}

	return nil
}

// ClusterAddHandler is the HTTP handler for ClusterAdd
func (h *Handler) ClusterAddHandler(w http.ResponseWriter, r *http.Request) {
	var req types.ClusterAddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Table == "" {
		errorResponse(w, http.StatusBadRequest, "table name is required")
		return
	}

	if err := h.ClusterAdd(req.Table); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	successResponse(w, fmt.Sprintf("table %s added to cluster %s", req.Table, h.clusterName))
}

// ClusterDrop removes a table from the cluster (must be done before DROP TABLE)
func (h *Handler) ClusterDrop(w http.ResponseWriter, r *http.Request) {
	var req types.ClusterDropRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Table == "" {
		errorResponse(w, http.StatusBadRequest, "table name is required")
		return
	}

	sql := fmt.Sprintf("ALTER CLUSTER %s DROP %s", h.clusterName, req.Table)
	slog.Debug("removing table from cluster", "table", req.Table, "cluster", h.clusterName, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		// Check if table is not in cluster (idempotent)
		if strings.Contains(err.Error(), "is not in cluster") {
			slog.Debug("table not in cluster (idempotent)", "table", req.Table, "cluster", h.clusterName)
			successResponse(w, fmt.Sprintf("table %s not in cluster %s", req.Table, h.clusterName))
			return
		}
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to remove table from cluster: %v", err))
		return
	}

	successResponse(w, fmt.Sprintf("table %s removed from cluster %s", req.Table, h.clusterName))
}

// ClusterStatus returns cluster status
func (h *Handler) ClusterStatus(w http.ResponseWriter, r *http.Request) {
	status, err := h.client.GetClusterStatus(h.clusterName)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to get cluster status: %v", err))
		return
	}
	jsonResponse(w, http.StatusOK, map[string]interface{}{"cluster": h.clusterName, "status": status})
}

// ClusterBootstrap creates a new cluster (idempotent - skips if already in cluster)
func (h *Handler) ClusterBootstrap() error {
	// Check if already in a cluster
	status, _ := h.client.GetClusterStatus(h.clusterName)
	if status == "primary" || status == "synced" {
		slog.Debug("already in cluster, skipping bootstrap", "cluster", h.clusterName, "status", status)
		return nil
	}

	sql := fmt.Sprintf("CREATE CLUSTER %s", h.clusterName)
	slog.Debug("bootstrapping cluster", "cluster", h.clusterName, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		// Check if cluster already exists
		if strings.Contains(err.Error(), "cluster with name") && strings.Contains(err.Error(), "already exists") {
			slog.Debug("cluster already exists (idempotent)", "cluster", h.clusterName)
			return nil
		}
		return fmt.Errorf("failed to create cluster: %w", err)
	}

	return nil
}

// ClusterBootstrapHandler is the HTTP handler for ClusterBootstrap
func (h *Handler) ClusterBootstrapHandler(w http.ResponseWriter, r *http.Request) {
	if err := h.ClusterBootstrap(); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	successResponse(w, fmt.Sprintf("cluster %s bootstrapped", h.clusterName))
}

// ClusterJoin joins an existing cluster (idempotent - skips if already in cluster)
func (h *Handler) ClusterJoin(sourceAddr string) error {
	// Check if already in a cluster
	status, _ := h.client.GetClusterStatus(h.clusterName)
	if status == "primary" || status == "synced" {
		slog.Debug("already in cluster, skipping join", "cluster", h.clusterName, "status", status)
		return nil
	}

	sql := fmt.Sprintf("JOIN CLUSTER %s AT '%s'", h.clusterName, sourceAddr)
	slog.Debug("joining cluster", "cluster", h.clusterName, "sourceAddr", sourceAddr, "sql", sql)

	if err := h.client.Execute(sql); err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}

	return nil
}

// ClusterJoinHandler is the HTTP handler for ClusterJoin
func (h *Handler) ClusterJoinHandler(w http.ResponseWriter, r *http.Request) {
	var req types.ClusterJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.SourceAddr == "" {
		errorResponse(w, http.StatusBadRequest, "sourceAddr is required")
		return
	}

	if err := h.ClusterJoin(req.SourceAddr); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	successResponse(w, fmt.Sprintf("joined cluster %s via %s", h.clusterName, req.SourceAddr))
}

// ClusterRejoinHandler forces this node to leave any current cluster and join the specified one
func (h *Handler) ClusterRejoinHandler(w http.ResponseWriter, r *http.Request) {
	var req types.ClusterRejoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.SourceAddr == "" {
		errorResponse(w, http.StatusBadRequest, "sourceAddr is required")
		return
	}

	if err := h.ClusterRejoin(req); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to join cluster: %v", err))
	}

	successResponse(w, fmt.Sprintf("rejoined cluster %s via %s", h.clusterName, req.SourceAddr))
}

func (h *Handler) ClusterRejoin(req types.ClusterRejoinRequest) error {
	slog.Info("rejoining cluster", "sourceAddr", req.SourceAddr)

	// Step 1: Delete current cluster membership (if any)
	// This removes local cluster state without affecting the source cluster
	deleteSQL := fmt.Sprintf("DELETE CLUSTER %s", h.clusterName)
	slog.Debug("deleting current cluster membership", "sql", deleteSQL)
	if err := h.client.Execute(deleteSQL); err != nil {
		// Ignore errors - cluster may not exist locally
		slog.Debug("DELETE CLUSTER returned (may be expected)", "error", err)
	}

	// Step 2: Join the source cluster
	joinSQL := fmt.Sprintf("JOIN CLUSTER %s AT '%s'", h.clusterName, req.SourceAddr)
	slog.Debug("joining cluster", "sql", joinSQL)
	if err := h.client.Execute(joinSQL); err != nil {
		return err
	}
	return nil
}

// GetTableSchema returns the schema of a table using DESCRIBE
func (h *Handler) GetTableSchema(w http.ResponseWriter, r *http.Request) {
	// Extract table name from URL path: /api/tables/{name}/schema
	path := r.URL.Path
	path = strings.TrimPrefix(path, "/api/tables/")
	path = strings.TrimSuffix(path, "/schema")
	tableName := path

	if tableName == "" {
		errorResponse(w, http.StatusBadRequest, "table name is required")
		return
	}

	// Query the delta table which has the actual column schema
	// (both main and delta tables have the same columns, but distributed tables don't expose columns)
	deltaTable := tableName + "_delta"
	columns, err := h.client.DescribeTable(deltaTable)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to describe table: %v", err))
		return
	}

	jsonResponse(w, http.StatusOK, schema.TableSchemaResponse{
		Table:   tableName,
		Columns: columns,
	})
}

// TableConfigResponse represents table behavior configuration
type TableConfigResponse struct {
	Table           string `json:"table"`
	ImportMethod    string `json:"importMethod"`
	ClusterMain     bool   `json:"clusterMain"`
	HAStrategy      string `json:"haStrategy"`
	AgentRetryCount int    `json:"agentRetryCount"`
}

// GetTableConfig returns behavior configuration for a table
func (h *Handler) GetTableConfig(w http.ResponseWriter, r *http.Request) {
	// Extract table name from URL path: /api/tables/{name}/config
	path := r.URL.Path
	path = strings.TrimPrefix(path, "/api/tables/")
	path = strings.TrimSuffix(path, "/config")
	tableName := path

	if tableName == "" {
		errorResponse(w, http.StatusBadRequest, "table name is required")
		return
	}

	schema, ok := h.registry.Get(tableName)
	if !ok {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("no schema found for table %s", tableName))
		return
	}

	jsonResponse(w, http.StatusOK, TableConfigResponse{
		Table:           tableName,
		ImportMethod:    schema.ImportMethod,
		ClusterMain:     schema.ClusterMain,
		HAStrategy:      schema.HAStrategy,
		AgentRetryCount: schema.AgentRetryCount,
	})
}

// StartImport starts an async import job and returns immediately with a job ID
func (h *Handler) StartImport(w http.ResponseWriter, r *http.Request) {
	var req types.ImportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Table == "" {
		errorResponse(w, http.StatusBadRequest, "table is required")
		return
	}

	// For indexer method with prebuilt index, CSV path is not required on agent side
	// (the CSV was already processed by the cron job)
	if req.PrebuiltIndexPath == "" && req.CSVPath == "" {
		errorResponse(w, http.StatusBadRequest, "csvPath is required (unless using prebuilt index)")
		return
	}

	// Validate schema exists
	tableSchema, ok := h.registry.GetForDerivedTable(req.Table)
	if !ok {
		baseName := manticore.ExtractBaseTableName(req.Table)
		errorResponse(w, http.StatusBadRequest, fmt.Sprintf("no schema found for table %s (base: %s)", req.Table, baseName))
		return
	}

	// Build full path and validate CSV exists (skip for prebuilt index imports)
	var csvFullPath string
	if req.PrebuiltIndexPath != "" {
		// Prebuilt index: validate the index path exists by checking for the .meta file
		// Manticore indexes are stored as multiple files with a common prefix (e.g., table.meta, table.0.spa, etc.)
		// The PrebuiltIndexPath is the prefix, not a file or directory itself
		metaPath := req.PrebuiltIndexPath + ".meta"
		if _, err := os.Stat(metaPath); err != nil {
			if os.IsNotExist(err) {
				errorResponse(w, http.StatusBadRequest, fmt.Sprintf("prebuilt index not found: %s (checked for %s)", req.PrebuiltIndexPath, metaPath))
				return
			}
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to stat prebuilt index: %v", err))
			return
		}
		slog.Debug("using prebuilt index", "path", req.PrebuiltIndexPath, "metaFile", metaPath)
	} else {
		// Bulk import: validate CSV exists
		csvFullPath = filepath.Join(h.s3Mount, req.CSVPath)
		if _, err := os.Stat(csvFullPath); err != nil {
			if os.IsNotExist(err) {
				errorResponse(w, http.StatusBadRequest, fmt.Sprintf("CSV file not found: %s", csvFullPath))
				return
			}
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to stat CSV file: %v", err))
			return
		}
	}

	// Determine worker count (API override > env default)
	workers := h.importWorkers
	if req.Workers > 0 {
		workers = req.Workers
	}

	// Determine batch size (API override > env default)
	batchSize := h.batchSize
	if req.BatchSize > 0 {
		batchSize = req.BatchSize
	}

	// Create job with new fields
	job, err := h.jobManager.CreateJob(req.Table, req.CSVPath, req.Cluster, req.Method, req.MemLimit, req.PrebuiltIndexPath, workers, batchSize)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("failed to create job: %v", err))
		return
	}

	// Handle resume from checkpoint
	if req.Resume {
		existingJob := h.jobManager.FindJobByTableAndPath(req.Table, req.CSVPath)
		if existingJob != nil && existingJob.LastLineNum > 0 {
			job.LastLineNum = existingJob.LastLineNum
			slog.Info("resuming import from checkpoint", "jobId", job.ID, "lastLineNum", job.LastLineNum, "oldJobId", existingJob.ID)
		}
	}

	// Start async execution
	go h.executeImportJob(job, csvFullPath, tableSchema)

	jsonResponse(w, http.StatusAccepted, types.StartImportResponse{JobID: job.ID})
}

// executeImportJob dispatches to the appropriate import method
func (h *Handler) executeImportJob(job *types.ImportJob, csvFullPath string, tableSchema *manticore.Schema) {
	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register cancel function with job manager
	h.jobManager.SetJobCancelFunc(job.ID, cancel)

	// Update status to running
	if err := h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusRunning, ""); err != nil {
		slog.Error("failed to update job status", "jobId", job.ID, "error", err)
		return
	}

	// Dispatch based on import method
	if job.Method == types.ImportMethodIndexer {
		h.executeIndexerImport(ctx, job, csvFullPath, tableSchema)
		return
	}

	// Default: bulk API import
	h.executeBulkImport(ctx, job, csvFullPath, tableSchema)
}

// executeBulkImport runs the import using a worker pool and HTTP bulk API
func (h *Handler) executeBulkImport(ctx context.Context, job *types.ImportJob, csvFullPath string, tableSchema *manticore.Schema) {
	slog.Info("starting bulk import job", "jobId", job.ID, "table", job.Table, "csv", csvFullPath,
		"cluster", job.Cluster, "workers", job.Workers, "batchSize", job.BatchSize, "resumeFrom", job.LastLineNum)

	// Check if CSV has a header row
	hasHeader, err := manticore.CSVHasHeader(csvFullPath)
	if err != nil {
		h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusFailed,
			fmt.Sprintf("failed to check CSV header: %v", err))
		return
	}

	// Create worker pool with checkpoint callback for persistence
	pool := NewImportWorkerPool(ctx, ImportOptions{
		Table:        job.Table,
		Cluster:      job.Cluster,
		Columns:      convertColumns(tableSchema.Columns),
		BatchSize:    job.BatchSize,
		WorkerCount:  job.Workers,
		MySQLHost:    h.client.Host(),
		HTTPPort:     h.client.HTTPPort(),
		ErrorLogPath: h.jobManager.ErrorLogPath(job.ID),
		SkipHeader:   hasHeader,
		OnCheckpoint: func(processed, failed, lastLine int64) {
			job.ProcessedLines = processed
			job.FailedLines = failed
			job.LastLineNum = lastLine
			if err := h.jobManager.UpdateJob(job); err != nil {
				slog.Warn("failed to persist checkpoint", "jobId", job.ID, "error", err)
			} else {
				slog.Debug("checkpoint persisted", "jobId", job.ID, "lastLineNum", lastLine)
			}
		},
	})

	// Run import
	runErr := pool.Run(csvFullPath, job.LastLineNum)

	// Get final progress
	processed, failed, lastLine := pool.Progress()

	// Update job with final stats
	job.ProcessedLines = processed
	job.FailedLines = failed
	job.LastLineNum = lastLine
	h.jobManager.UpdateJob(job)

	// Check if cancelled
	if runErr == context.Canceled {
		h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusCancelled, "")
		slog.Info("bulk import job was cancelled", "jobId", job.ID, "lastLineNum", lastLine)
		return
	}

	// Check for errors
	if runErr != nil {
		h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusFailed,
			fmt.Sprintf("import failed: %v (see %s.error.log)", runErr, job.ID))
		return
	}

	// Success
	h.jobManager.UpdateJobStatus(job.ID, types.ImportJobStatusCompleted, "")
	slog.Info("bulk import job completed successfully", "jobId", job.ID,
		"processedLines", processed, "failedLines", failed)
}

// GetImportStatus returns the status of an import job
func (h *Handler) GetImportStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	if jobID == "" {
		errorResponse(w, http.StatusBadRequest, "jobId is required")
		return
	}

	job := h.jobManager.GetJob(jobID)
	if job == nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("job not found: %s", jobID))
		return
	}

	jsonResponse(w, http.StatusOK, types.ImportJobResponse{Job: job})
}

// CancelImport cancels a running import job
func (h *Handler) CancelImport(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	if jobID == "" {
		errorResponse(w, http.StatusBadRequest, "jobId is required")
		return
	}

	if err := h.jobManager.CancelJob(jobID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		errorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	successResponse(w, fmt.Sprintf("job %s cancelled", jobID))
}
