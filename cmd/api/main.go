package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	actions2 "github.com/controlplane-com/manticore-orchestrator/pkg/api/actions"
	"github.com/controlplane-com/manticore-orchestrator/pkg/api/client"
	"github.com/controlplane-com/manticore-orchestrator/pkg/indexer"
	"github.com/controlplane-com/manticore-orchestrator/pkg/s3"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/cluster"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/cpln"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron/v3"
)

// BackupScheduleEntry represents a scheduled backup configuration
type BackupScheduleEntry struct {
	Table    string `json:"table"`    // e.g., "addresses"
	Type     string `json:"type"`     // "main" or "delta"
	Schedule string `json:"schedule"` // 5-field cron: "0 */6 * * *"
}

// Config holds the orchestrator configuration
type Config struct {
	AgentPort            string
	WorkloadName         string
	GVC                  string
	Location             string
	Org                  string
	TablesConfig         string
	AuthToken            string
	CplnToken            string
	ListenAddr           string
	BootstrapTimeout     int    // Seconds to wait before bootstrapping a lone replica
	OrchestratorWorkload string // Name of the cron workload for triggering imports
	BackupWorkload       string // Name of the cron workload for triggering backups
	BackupContainer      string // Container name in the backup workload (default "backup")
	ManticoreMySQLPort   string // Port for direct MySQL connections (default 9306)

	// S3 configuration for indexer method
	S3Bucket        string // S3 bucket for index uploads
	S3Region        string // AWS region for S3
	S3IndexPrefix   string // Path prefix for index uploads (default: "indexer-output")
	S3Mount         string // Mount path agents use for S3 (default: "/mnt/s3")
	IndexerWorkDir  string // Local temp directory for indexer builds (default: "/tmp/indexer")
	IndexerMemLimit string // Memory limit for indexer (default: "2G")

	// Shared volume configuration (alternative to S3 for indexer output)
	SharedVolumeMount string // Mount path for shared volume (e.g., "/mnt/shared") - same path on cron and agents

	// Backup storage configuration (for listing/restore)
	BackupProvider string // "aws" or "gcp" - determines which SDK to use
	BackupBucket   string // Bucket name for backups
	BackupPrefix   string // Prefix within bucket (e.g., "backups")
	BackupRegion   string // AWS region for S3 backups

	// Backup scheduling configuration
	BackupSchedules []BackupScheduleEntry // from BACKUP_SCHEDULES env var (JSON array)
}

// Server is the orchestrator REST API server
type Server struct {
	config        Config
	cplnClient    *cpln.Client
	backupClient  *s3.Client // S3 client for backup operations (also works for S3-compatible storage)
	mu            sync.RWMutex
	cronScheduler *cron.Cron // Background backup scheduler (nil if no schedules configured)
}

// TableConfig represents a table configuration entry
type TableConfig struct {
	Name    string `json:"name"`
	CsvPath string `json:"csvPath"`
}

// QueryRequest represents the request for /api/query
type QueryRequest struct {
	Query        string `json:"query"`
	ReplicaIndex *int   `json:"replicaIndex,omitempty"` // nil = load balanced
	Broadcast    bool   `json:"broadcast,omitempty"`    // query all replicas
}

// ColumnMeta represents column metadata in query results
type ColumnMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryResponse represents the response for a single replica query
type QueryResponse struct {
	Status          string                   `json:"status"` // "ok" or "error"
	Columns         []ColumnMeta             `json:"columns,omitempty"`
	Rows            []map[string]interface{} `json:"rows,omitempty"`
	RowCount        int                      `json:"rowCount,omitempty"`
	ExecutionTimeMs int64                    `json:"executionTimeMs,omitempty"`
	ReplicaIndex    *int                     `json:"replicaIndex,omitempty"`
	Error           string                   `json:"error,omitempty"`
}

// ReplicaQueryResult represents the result from a single replica in broadcast mode
type ReplicaQueryResult struct {
	ReplicaIndex    int                      `json:"replicaIndex"`
	Status          string                   `json:"status"` // "success" or "error"
	Columns         []ColumnMeta             `json:"columns,omitempty"`
	Rows            []map[string]interface{} `json:"rows,omitempty"`
	RowCount        int                      `json:"rowCount,omitempty"`
	ExecutionTimeMs int64                    `json:"executionTimeMs,omitempty"`
	Error           string                   `json:"error,omitempty"`
}

// BroadcastQueryResponse represents the response for broadcast queries
type BroadcastQueryResponse struct {
	Status  string               `json:"status"`
	Results []ReplicaQueryResult `json:"results"`
}

func main() {
	// Setup structured logging
	logLevel := getEnv("LOG_LEVEL", "info")
	var level slog.Level
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	mode := getEnv("MODE", "cli") // "server" or "cli"

	config := Config{
		AgentPort:            getEnv("AGENT_PORT", "8080"),
		WorkloadName:         getEnv("WORKLOAD_NAME", "manticore"),
		GVC:                  extractName(getEnv("CPLN_GVC", "")),
		Location:             extractName(getEnv("CPLN_LOCATION", "")),
		Org:                  extractName(getEnv("CPLN_ORG", "")),
		TablesConfig:         getEnv("TABLES_CONFIG", "{}"),
		AuthToken:            getEnv("AUTH_TOKEN", ""),
		CplnToken:            getEnv("CPLN_TOKEN", ""),
		ListenAddr:           getEnv("LISTEN_ADDR", ":8080"),
		BootstrapTimeout:     getEnvInt("BOOTSTRAP_TIMEOUT", 60),
		OrchestratorWorkload: getEnv("ORCHESTRATOR_WORKLOAD", ""),
		BackupWorkload:       getEnv("BACKUP_WORKLOAD", ""),
		BackupContainer:      getEnv("BACKUP_CONTAINER_NAME", "backup"),
		ManticoreMySQLPort:   getEnv("MANTICORE_MYSQL_PORT", "9306"),
		// S3 configuration for indexer method
		S3Bucket:        getEnv("S3_BUCKET", ""),
		S3Region:        getEnv("S3_REGION", "us-east-1"),
		S3IndexPrefix:   getEnv("S3_INDEX_PREFIX", "indexer-output"),
		S3Mount:         getEnv("S3_MOUNT", "/mnt/s3"),
		IndexerWorkDir:  getEnv("INDEXER_WORK_DIR", "/tmp/indexer"),
		IndexerMemLimit: getEnv("INDEXER_MEM_LIMIT", "2G"),
		// Shared volume configuration (alternative to S3)
		SharedVolumeMount: getEnv("SHARED_VOLUME_MOUNT", ""),
		// Backup storage configuration
		BackupProvider: getEnv("BACKUP_PROVIDER", "aws"),
		BackupBucket:   getEnv("BACKUP_BUCKET", ""),
		BackupPrefix:   getEnv("BACKUP_PREFIX", "backups"),
		BackupRegion:   getEnv("BACKUP_REGION", "us-east-1"),
	}

	// Parse backup schedules from JSON env var
	if raw := getEnv("BACKUP_SCHEDULES", ""); raw != "" {
		slog.Info("parsing BACKUP_SCHEDULES", "raw", raw)
		if err := json.Unmarshal([]byte(raw), &config.BackupSchedules); err != nil {
			slog.Error("failed to parse BACKUP_SCHEDULES", "error", err, "raw", raw)
			os.Exit(1)
		}
		slog.Info("parsed backup schedules", "count", len(config.BackupSchedules))
	} else {
		slog.Info("BACKUP_SCHEDULES not configured, scheduled backups disabled")
	}

	if config.AuthToken == "" {
		slog.Error("AUTH_TOKEN environment variable is required")
		os.Exit(1)
	}

	if mode == "server" {
		runServer(config)
	} else {
		runCLI(config)
	}
}

// runServer starts the REST API server
func runServer(config Config) {
	var cplnClient *cpln.Client
	if config.CplnToken != "" && config.Org != "" {
		cplnClient = cpln.NewClient(config.CplnToken, config.Org)
		slog.Info("Control Plane API client initialized", "org", config.Org)
	} else {
		slog.Warn("CPLN_TOKEN or CPLN_ORG not set, some features will be limited")
	}

	// Initialize backup storage client if configured
	var backupClient *s3.Client
	if config.BackupBucket != "" && config.BackupProvider == "aws" {
		var err error
		backupClient, err = s3.NewClient(config.BackupBucket, config.BackupRegion)
		if err != nil {
			slog.Warn("failed to create backup S3 client", "error", err)
		} else {
			slog.Info("backup S3 client initialized", "bucket", config.BackupBucket, "prefix", config.BackupPrefix)
		}
	}

	server := &Server{
		config:       config,
		cplnClient:   cplnClient,
		backupClient: backupClient,
	}

	// Start background repair loop
	/*
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for range ticker.C {
				replicaCount, err := server.getReplicaCount()
				if err != nil {
					slog.Debug("repair loop: failed to get replica count", "error", err)
					continue
				}

				clients := server.buildClients(replicaCount)
				ctx := &actions.Context{
					Clients: clients,
				}

				if err := actions.Repair(ctx); err != nil {
					slog.Debug("repair loop: repair failed", "error", err)
				}
			}
		}()
	*/

	mux := http.NewServeMux()

	// Apply auth middleware to all endpoints
	loggedMux := requestLogger(mux)
	authMux := authMiddleware(config.AuthToken, loggedMux)

	// Health endpoint (legacy - simple health check)
	mux.HandleFunc("/api/health", server.handleHealth)

	// Repair endpoint (no table required)
	mux.HandleFunc("/api/repair", server.handleRepair)

	// Table-specific endpoints
	mux.HandleFunc("/api/init", server.handleInit)
	mux.HandleFunc("/api/import", server.handleImport)

	// New endpoints for UI
	mux.HandleFunc("/api/config", server.handleConfig)
	mux.HandleFunc("/api/cluster", server.handleCluster)
	mux.HandleFunc("/api/cluster/discover", server.handleClusterDiscover)
	mux.HandleFunc("/api/cluster/query-counts", server.handleClusterQueryCounts)
	mux.HandleFunc("/api/tables/status", server.handleTablesStatus)
	mux.HandleFunc("/api/tables/", server.handleTableSchema)
	mux.HandleFunc("/api/backup", server.handleBackup)
	mux.HandleFunc("/api/imports", server.handleImports)
	mux.HandleFunc("/api/backups", server.handleBackups)
	mux.HandleFunc("/api/backups/files", server.handleBackupFiles)
	mux.HandleFunc("/api/restore", server.handleRestore)
	mux.HandleFunc("/api/rotate-main", server.handleRotateMain)
	mux.HandleFunc("/api/repairs", server.handleRepairs)
	mux.HandleFunc("/api/commands", server.handleCommands)
	mux.HandleFunc("/api/commands/retry", server.handleCommandRetry)
	mux.HandleFunc("/api/query", server.handleQuery)

	// Status endpoint (unauthenticated, for readiness probes)
	http.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	// All other endpoints require auth
	http.Handle("/api/", authMux)

	// Start backup scheduler if schedules are configured
	if len(config.BackupSchedules) > 0 {
		server.cronScheduler = cron.New()
		for _, entry := range config.BackupSchedules {
			entry := entry // capture loop var
			entryID, err := server.cronScheduler.AddFunc(entry.Schedule, func() {
				slog.Info("cron triggered backup", "table", entry.Table, "type", entry.Type, "schedule", entry.Schedule)
				if _, err := server.triggerBackup(entry.Table, entry.Type); err != nil {
					slog.Error("scheduled backup failed", "table", entry.Table, "type", entry.Type, "error", err)
				} else {
					slog.Info("scheduled backup triggered successfully", "table", entry.Table, "type", entry.Type)
				}
			})
			if err != nil {
				slog.Error("invalid cron schedule, skipping entry", "table", entry.Table, "type", entry.Type, "schedule", entry.Schedule, "error", err)
				continue
			}
			slog.Info("registered backup schedule", "table", entry.Table, "type", entry.Type, "schedule", entry.Schedule, "entryId", entryID)
		}
		server.cronScheduler.Start()

		// Log next fire time for each registered schedule
		for _, cronEntry := range server.cronScheduler.Entries() {
			slog.Info("scheduled backup next fire", "entryId", cronEntry.ID, "nextRun", cronEntry.Next.Format(time.RFC3339))
		}
		slog.Info("backup scheduler started", "scheduleCount", len(config.BackupSchedules))
	} else {
		slog.Info("no backup schedules configured, scheduler not started")
	}

	// Start HTTP server with graceful shutdown
	srv := &http.Server{Addr: config.ListenAddr, Handler: nil}

	go func() {
		slog.Info("Manticore Orchestrator REST API starting", "addr", config.ListenAddr, "workload", config.WorkloadName, "gvc", config.GVC, "location", config.Location)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server...")
	if server.cronScheduler != nil {
		server.cronScheduler.Stop()
		slog.Info("backup scheduler stopped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	slog.Info("server stopped")
}

// getOrchestratorWorkload returns the orchestrator cron workload name
func (s *Server) getOrchestratorWorkload() string {
	if s.config.OrchestratorWorkload != "" {
		return s.config.OrchestratorWorkload
	}
	return strings.TrimSuffix(s.config.WorkloadName, "-manticore") + "-orchestrator-api"
}

// getBackupWorkload returns the backup cron workload name
func (s *Server) getBackupWorkload() string {
	if s.config.BackupWorkload != "" {
		return s.config.BackupWorkload
	}
	return s.config.WorkloadName + "-backup"
}

// getReplicaCount fetches the replica count from Control Plane API
func (s *Server) getReplicaCount() (int, error) {
	if s.cplnClient == nil {
		return 0, fmt.Errorf("Control Plane client not configured (CPLN_TOKEN/CPLN_ORG required)")
	}

	return s.cplnClient.GetReplicaCount(s.config.GVC, s.config.WorkloadName)
}

// buildClients creates agent clients for the specified replica count
func (s *Server) buildClients(replicaCount int) []*client.AgentClient {
	var clients []*client.AgentClient
	for i := 0; i < replicaCount; i++ {
		endpoint := fmt.Sprintf("http://%s-%d.%s:%s",
			s.config.WorkloadName, i, s.config.WorkloadName, s.config.AgentPort)
		clients = append(clients, client.NewAgentClient(endpoint, s.config.AuthToken))
	}
	return clients
}

// getTablesConfig parses the TABLES_CONFIG JSON
func (s *Server) getTablesConfig() ([]TableConfig, error) {
	var configMap map[string]string
	if err := json.Unmarshal([]byte(s.config.TablesConfig), &configMap); err != nil {
		return nil, fmt.Errorf("failed to parse TABLES_CONFIG: %w", err)
	}

	var tables []TableConfig
	for name, csvPath := range configMap {
		tables = append(tables, TableConfig{Name: name, CsvPath: csvPath})
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	return tables, nil
}

// requestLogger logs all incoming API requests
func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.Info("incoming request", "method", r.Method, "path", r.URL.Path, "remoteAddr", r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

// authMiddleware validates bearer token
func authMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, `{"error":"missing Authorization header"}`, http.StatusUnauthorized)
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			http.Error(w, `{"error":"invalid Authorization header format"}`, http.StatusUnauthorized)
			return
		}

		if parts[1] != token {
			http.Error(w, `{"error":"invalid token"}`, http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// handleConfig handles GET /api/config - returns cluster configuration
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	tables, err := s.getTablesConfig()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"replicaCount": replicaCount,
		"workloadName": s.config.WorkloadName,
		"gvc":          s.config.GVC,
		"location":     s.config.Location,
		"tables":       tables,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// ReplicaStatus represents the status of a single replica
type ReplicaStatus struct {
	Index             int     `json:"index"`
	Endpoint          string  `json:"endpoint"`
	Status            string  `json:"status"` // online, offline, not_in_use, error
	ClusterStatus     *string `json:"clusterStatus"`
	NodeState         *string `json:"nodeState"`
	Error             *string `json:"error"`
	DeploymentMessage *string `json:"deploymentMessage,omitempty"`
}

// classifyReplicaError determines the status and user-friendly error message for a replica error
func classifyReplicaError(err error) (status, message string) {
	errStr := err.Error()
	if strings.Contains(errStr, "no such host") {
		return "not_in_use", "replica is not in use"
	}
	if strings.Contains(errStr, "API error (503)") {
		return "error", "replica returned an error"
	}
	return "offline", "replica is offline"
}

// ClusterResponse represents the cluster status response
type ClusterResponse struct {
	Status   string          `json:"status"` // healthy, degraded, uninitialized
	Replicas []ReplicaStatus `json:"replicas"`
}

// handleCluster handles GET /api/cluster - returns detailed replica status
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	clients := s.buildClients(replicaCount)
	replicas := make([]ReplicaStatus, len(clients))

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(idx int, agentClient *client.AgentClient) {
			defer wg.Done()

			replica := ReplicaStatus{
				Index:    idx,
				Endpoint: agentClient.BaseURL(),
				Status:   "offline",
			}

			health, err := agentClient.Health(1) // No retries for REST API
			if err != nil {
				status, message := classifyReplicaError(err)
				replica.Status = status
				replica.Error = &message
			} else {
				replica.Status = "online"
				if health.ClusterStatus != "" {
					replica.ClusterStatus = &health.ClusterStatus
				}
			}

			replicas[idx] = replica
		}(i, c)
	}
	wg.Wait()

	// Check if we need deployment info for unhealthy replicas
	hasUnhealthyInUse := false
	for _, r := range replicas {
		if r.Status != "not_in_use" && r.Status != "online" {
			hasUnhealthyInUse = true
			break
		}
		if r.Status == "online" && r.ClusterStatus != nil {
			cs := *r.ClusterStatus
			if cs != "primary" && cs != "synced" {
				hasUnhealthyInUse = true
				break
			}
		}
	}

	// Fetch deployment messages for unhealthy replicas
	if hasUnhealthyInUse && s.cplnClient != nil {
		s.enrichWithDeploymentMessages(replicas)
	}

	// Determine overall status
	status := "uninitialized"
	onlineCount := 0
	inUseCount := 0
	hasCluster := false
	allHealthy := true

	for _, r := range replicas {
		if r.Status != "not_in_use" {
			inUseCount++
		}
		if r.Status == "online" {
			onlineCount++
			if r.ClusterStatus != nil {
				hasCluster = true
				cs := *r.ClusterStatus
				if cs != "primary" && cs != "synced" {
					allHealthy = false
				}
			}
		}
	}

	if hasCluster {
		if onlineCount == inUseCount && allHealthy {
			status = "healthy"
		} else {
			status = "degraded"
		}
	}

	response := ClusterResponse{
		Status:   status,
		Replicas: replicas,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// enrichWithDeploymentMessages fetches deployment info and adds messages to unhealthy replicas
func (s *Server) enrichWithDeploymentMessages(replicas []ReplicaStatus) {
	deployments, err := s.cplnClient.GetDeployments(s.config.GVC, s.config.WorkloadName)
	if err != nil {
		slog.Debug("failed to fetch deployments for health enrichment", "error", err)
		return
	}

	// Find deployment for current location
	var currentDeployment *cpln.Deployment
	for i := range deployments.Items {
		if deployments.Items[i].Name == s.config.Location {
			currentDeployment = &deployments.Items[i]
			break
		}
	}

	if currentDeployment == nil {
		return
	}

	// Match versions to replicas by index
	for i := range replicas {
		// Skip healthy or not-in-use replicas
		if replicas[i].Status == "not_in_use" {
			continue
		}
		if replicas[i].Status == "online" {
			if replicas[i].ClusterStatus != nil {
				cs := *replicas[i].ClusterStatus
				if cs == "primary" || cs == "synced" {
					continue
				}
			}
		}

		// Get version for this replica index
		if i < len(currentDeployment.Status.Versions) {
			version := currentDeployment.Status.Versions[i]
			msg := buildDeploymentMessage(version)
			if msg != "" {
				replicas[i].DeploymentMessage = &msg
			}
		}
	}
}

// buildDeploymentMessage constructs a message from deployment version info
func buildDeploymentMessage(version cpln.DeploymentVersion) string {
	var messages []string

	// Add version-level message
	if version.Message != "" {
		messages = append(messages, version.Message)
	}

	// Add container-level messages
	for _, container := range version.Containers {
		if container.Message != "" && container.Message != version.Message {
			messages = append(messages, container.Message)
		}
	}

	return strings.Join(messages, "\n")
}

// QueryCountResponse represents query count for a single replica
type QueryCountResponse struct {
	Index     int    `json:"index"`
	Endpoint  string `json:"endpoint"`
	QueryCount *int64 `json:"queryCount,omitempty"`
	Error     *string `json:"error,omitempty"`
}

// handleClusterQueryCounts handles GET /api/cluster/query-counts - returns query counts for all replicas
func (s *Server) handleClusterQueryCounts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	clients := s.buildClients(replicaCount)
	results := make([]QueryCountResponse, len(clients))

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(idx int, agentClient *client.AgentClient) {
			defer wg.Done()

			result := QueryCountResponse{
				Index:    idx,
				Endpoint: agentClient.BaseURL(),
			}

			count, err := agentClient.QueryCount(1) // No retries for REST API
			if err != nil {
				// Use same logic as replica status - don't show error for "not_in_use" replicas
				status, _ := classifyReplicaError(err)
				if status != "not_in_use" {
					errMsg := err.Error()
					result.Error = &errMsg
				}
				// If status is "not_in_use", leave Error as nil (don't display error)
			} else {
				result.QueryCount = &count
			}

			results[idx] = result
		}(i, c)
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// ClusterDiscoverResponse represents the response for /api/cluster/discover
type ClusterDiscoverResponse struct {
	Cluster    *cluster.ClusterDesc   `json:"cluster"`
	SplitBrain bool                   `json:"splitBrain"`
	Groups     []cluster.ClusterGroup `json:"groups,omitempty"`
}

// handleClusterDiscover handles GET /api/cluster/discover - finds the winning cluster to join
func (s *Server) handleClusterDiscover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	clients := s.buildClients(replicaCount)

	// Collect replica info concurrently
	replicas := make([]cluster.ReplicaInfo, len(clients))
	var wg sync.WaitGroup

	for i, c := range clients {
		wg.Add(1)
		go func(idx int, agentClient *client.AgentClient) {
			defer wg.Done()

			replica := cluster.ReplicaInfo{Index: idx}

			// Get health
			health, err := agentClient.Health(1) // No retries for REST API
			if err != nil {
				slog.Debug("cluster discover: replica health check failed", "replica", idx, "error", err)
				replica.Reachable = false
				replicas[idx] = replica
				return
			}
			replica.Reachable = true
			replica.ClusterStatus = health.ClusterStatus

			// Get grastate
			gs, err := agentClient.Grastate(1) // No retries for REST API
			if err != nil {
				slog.Debug("cluster discover: replica grastate failed", "replica", idx, "error", err)
				replicas[idx] = replica
				return
			}

			replica.UUID = gs.UUID
			replica.Seqno = gs.Seqno
			replica.HasValidUUID = gs.Exists && gs.UUID != "" && gs.UUID != "00000000-0000-0000-0000-000000000000"

			replicas[idx] = replica
		}(i, c)
	}
	wg.Wait()

	// Find winning cluster
	desc := cluster.FindWinningCluster(replicas, s.config.WorkloadName, "9312")

	// Find all UUID groups for split-brain detection
	groups := cluster.FindAllClusterGroups(replicas)
	splitBrain := len(groups) > 1

	response := ClusterDiscoverResponse{
		Cluster:    desc,
		SplitBrain: splitBrain,
		Groups:     groups,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// TableComponentStatus represents the status of a table component (main, delta, distributed)
type TableComponentStatus struct {
	Present   bool `json:"present"`
	InCluster bool `json:"inCluster"`
}

// TableReplicaStatus represents table status on a single replica
type TableReplicaStatus struct {
	Index            int                  `json:"index"`
	Online           bool                 `json:"online"`
	MainTable        TableComponentStatus `json:"mainTable"`
	DeltaTable       TableComponentStatus `json:"deltaTable"`
	DistributedTable TableComponentStatus `json:"distributedTable"`
	Error            *string              `json:"error,omitempty"`
}

// TableStatusEntry represents the status of a single table across all replicas
type TableStatusEntry struct {
	Name        string               `json:"name"`
	CsvPath     string               `json:"csvPath"`
	ClusterMain bool                 `json:"clusterMain"` // Whether main table should be in cluster
	Replicas    []TableReplicaStatus `json:"replicas"`
}

// TablesStatusResponse represents the response for /api/tables/status
type TablesStatusResponse struct {
	TableSlots map[string]string  `json:"tableSlots"`
	Tables     []TableStatusEntry `json:"tables"`
}

// handleTablesStatus handles GET /api/tables/status - returns per-replica table status
func (s *Server) handleTablesStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	tablesConfig, err := s.getTablesConfig()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	clients := s.buildClients(replicaCount)

	// Discover active slots by querying agents for existing main tables
	var tableNames []string
	for _, t := range tablesConfig {
		tableNames = append(tableNames, t.Name)
	}
	tableSlots := actions2.DiscoverTableSlots(clients, tableNames)

	// Fetch table configs to get clusterMain setting for each table
	// Try first available replica for each table config
	tableConfigs := make(map[string]*client.TableConfigResponse)
	for _, t := range tablesConfig {
		for _, c := range clients {
			cfg, err := c.GetTableConfig(t.Name, 1)
			if err == nil {
				tableConfigs[t.Name] = cfg
				break
			}
		}
	}

	// Fetch tables from each replica concurrently
	type replicaTables struct {
		index  int
		tables []types.TableInfo
		err    error
	}
	results := make(chan replicaTables, len(clients))

	for i, c := range clients {
		go func(idx int, agentClient *client.AgentClient) {
			tables, err := agentClient.ListTables(1) // No retries for REST API
			results <- replicaTables{index: idx, tables: tables, err: err}
		}(i, c)
	}

	// Collect results
	replicaTableMap := make(map[int][]types.TableInfo)
	replicaErrors := make(map[int]error)
	for i := 0; i < len(clients); i++ {
		result := <-results
		if result.err != nil {
			replicaErrors[result.index] = result.err
		} else {
			replicaTableMap[result.index] = result.tables
		}
	}

	// Build response for each configured table
	var tableEntries []TableStatusEntry
	for _, tableConfig := range tablesConfig {
		// Default clusterMain to true if we couldn't fetch config
		clusterMain := true
		if cfg, ok := tableConfigs[tableConfig.Name]; ok {
			clusterMain = cfg.ClusterMain
		}

		entry := TableStatusEntry{
			Name:        tableConfig.Name,
			CsvPath:     tableConfig.CsvPath,
			ClusterMain: clusterMain,
			Replicas:    make([]TableReplicaStatus, replicaCount),
		}

		// Look up the active slot for this specific table (default to "a")
		activeSlot := tableSlots[tableConfig.Name]
		if activeSlot == "" {
			activeSlot = "a"
		}
		mainTableName := fmt.Sprintf("%s_main_%s", tableConfig.Name, activeSlot)
		deltaTableName := fmt.Sprintf("%s_delta", tableConfig.Name)
		distributedTableName := tableConfig.Name

		for idx := 0; idx < replicaCount; idx++ {
			replicaStatus := TableReplicaStatus{
				Index:  idx,
				Online: true,
			}

			if err, ok := replicaErrors[idx]; ok {
				replicaStatus.Online = false
				errStr := err.Error()
				replicaStatus.Error = &errStr
			} else if tables, ok := replicaTableMap[idx]; ok {
				// Check for each table component
				for _, t := range tables {
					switch t.Name {
					case mainTableName:
						replicaStatus.MainTable.Present = true
						replicaStatus.MainTable.InCluster = t.InCluster
					case deltaTableName:
						replicaStatus.DeltaTable.Present = true
						replicaStatus.DeltaTable.InCluster = t.InCluster
					case distributedTableName:
						replicaStatus.DistributedTable.Present = true
						// Distributed tables are not in the cluster
					}
				}
			}

			entry.Replicas[idx] = replicaStatus
		}

		tableEntries = append(tableEntries, entry)
	}

	response := TablesStatusResponse{
		TableSlots: tableSlots,
		Tables:     tableEntries,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleTableSchema handles GET /api/tables/{name}/schema - proxies to first online replica
func (s *Server) handleTableSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Extract table name from URL path: /api/tables/{name}/schema
	path := strings.TrimPrefix(r.URL.Path, "/api/tables/")
	path = strings.TrimSuffix(path, "/schema")
	tableName := path

	if tableName == "" || !strings.HasSuffix(r.URL.Path, "/schema") {
		jsonError(w, http.StatusBadRequest, "invalid path - expected /api/tables/{name}/schema")
		return
	}

	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	clients := s.buildClients(replicaCount)

	// Find first online replica and get schema
	var lastErr error
	for idx, agentClient := range clients {
		schema, err := agentClient.GetTableSchema(tableName, 1) // No retries
		if err != nil {
			lastErr = err
			slog.Debug("table schema: replica failed", "replica", idx, "error", err)
			continue
		}

		// Success - return the schema
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(schema)
		return
	}

	// All replicas failed
	if lastErr != nil {
		jsonError(w, http.StatusServiceUnavailable, fmt.Sprintf("failed to get table schema from any replica: %v", lastErr))
	} else {
		jsonError(w, http.StatusServiceUnavailable, "no replicas available")
	}
}

// handleHealth handles GET /api/health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	clients := s.buildClients(replicaCount)

	ctx := &actions2.Context{
		Clients: clients,
	}

	if err := actions2.Health(ctx); err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	jsonSuccess(w, "health check completed")
}

// handleRepair handles POST /api/repair - triggers cron workload for repair
func (s *Server) handleRepair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Check CPLN client is configured
	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	// Parse request body
	var req struct {
		SourceReplica *int `json:"sourceReplica"` // nil means intelligent selection
	}
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req) // Ignore error, optional field
	}

	orchestratorWorkload := s.getOrchestratorWorkload()

	// Check for in-progress imports or repairs
	commands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, orchestratorWorkload, 0)
	if err != nil {
		slog.Warn("failed to query active commands", "error", err)
		// Continue anyway - better to allow potential duplicate than block all repairs
	} else {
		for _, cmd := range commands.Items {
			if cmd.Type == "runCronWorkload" {
				action := extractActionFromCommand(cmd)
				if action == "import" {
					jsonError(w, http.StatusConflict,
						fmt.Sprintf("import in progress (command %s), cannot start repair", cmd.ID))
					return
				}
				if action == "repair" {
					jsonError(w, http.StatusConflict,
						fmt.Sprintf("repair already in progress (command %s)", cmd.ID))
					return
				}
			}
		}
	}

	// Check for active backups on the backup workload
	backupWorkload := s.getBackupWorkload()
	backupCommands, backupErr := s.cplnClient.QueryActiveCommands(s.config.GVC, backupWorkload, 0)
	if backupErr != nil {
		slog.Warn("failed to query active backup commands", "error", backupErr)
	} else {
		for _, cmd := range backupCommands.Items {
			if cmd.Type == "runCronWorkload" {
				action := extractActionFromCommand(cmd)
				if action == "backup" {
					jsonError(w, http.StatusConflict,
						fmt.Sprintf("backup in progress (command %s), cannot start repair", cmd.ID))
					return
				}
			}
		}
	}

	// Build env overrides
	envVars := []cpln.EnvVar{{Name: "ACTION", Value: "repair"}}
	if req.SourceReplica != nil {
		envVars = append(envVars, cpln.EnvVar{
			Name:  "REPAIR_SOURCE_REPLICA",
			Value: strconv.Itoa(*req.SourceReplica),
		})
	}

	overrides := []cpln.ContainerOverride{
		{
			Name: "orchestrator",
			Env:  envVars,
		},
	}

	slog.Info("triggering repair via cron workload", "sourceReplica", req.SourceReplica, "workload", orchestratorWorkload)
	cmd, err := s.cplnClient.StartCronWorkload(s.config.GVC, orchestratorWorkload, s.config.Location, overrides)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to start repair: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "accepted",
		"message":   "repair started",
		"commandId": cmd.ID,
	})
}

// handleInit handles POST /api/init
// Called by each agent at startup to initialize the cluster and tables
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req struct {
		ReplicaIndex    int    `json:"replicaIndex"`
		AliveSinceStart int    `json:"aliveSinceStart"` // Seconds since agent started
		ClusterStatus   string `json:"clusterStatus"`   // "primary", "non-primary", or ""
		NodeState       string `json:"nodeState"`       // "synced", "donor", etc.
		Grastate        *struct {
			UUID            string `json:"uuid"`
			Seqno           int64  `json:"seqno"`
			SafeToBootstrap int    `json:"safeToBootstrap"`
			Exists          bool   `json:"exists"`
		} `json:"grastate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Get all configured tables
	tables, err := s.getTablesConfig()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get tables config: %v", err))
		return
	}

	if len(tables) == 0 {
		jsonError(w, http.StatusBadRequest, "no tables configured in TABLES_CONFIG")
		return
	}

	// Convert TableConfig to actions.TableConfig
	actionTables := make([]actions2.TableConfig, len(tables))
	for i, t := range tables {
		actionTables[i] = actions2.TableConfig{Name: t.Name, CsvPath: t.CsvPath}
	}

	// Convert grastate to CallerInfo
	var callerInfo *actions2.CallerInfo
	if req.Grastate != nil {
		callerInfo = &actions2.CallerInfo{
			UUID:            req.Grastate.UUID,
			Seqno:           req.Grastate.Seqno,
			SafeToBootstrap: req.Grastate.SafeToBootstrap,
			Exists:          req.Grastate.Exists,
			ClusterStatus:   req.ClusterStatus,
			NodeState:       req.NodeState,
		}
	}

	// Create closures for dynamic client building and replica count fetching
	clientBuilder := func(replicaCount int) []*client.AgentClient {
		return s.buildClients(replicaCount)
	}
	replicaCountFetcher := func() (int, error) {
		return s.getReplicaCount()
	}

	slog.Info("init requested", "replicaIndex", req.ReplicaIndex, "tables", len(tables), "hasGrastate", callerInfo != nil, "aliveSinceStart", req.AliveSinceStart, "clusterStatus", req.ClusterStatus, "nodeState", req.NodeState)
	result, err := actions2.Init(r.Context(), clientBuilder, replicaCountFetcher, actionTables, req.ReplicaIndex, callerInfo, req.AliveSinceStart, s.config.BootstrapTimeout)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Return instructions for agent to execute
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// extractTableNameFromCommand extracts the TABLE_NAME env var from a runCronWorkload command
func extractTableNameFromCommand(cmd cpln.Command) string {
	overrides, ok := cmd.Spec["containerOverrides"].([]interface{})
	if !ok {
		return ""
	}
	for _, override := range overrides {
		co, ok := override.(map[string]interface{})
		if !ok {
			continue
		}
		env, ok := co["env"].([]interface{})
		if !ok {
			continue
		}
		for _, e := range env {
			ev, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			if ev["name"] == "TABLE_NAME" {
				if val, ok := ev["value"].(string); ok {
					return val
				}
			}
		}
	}
	return ""
}

// extractActionFromCommand extracts the ACTION env var from a runCronWorkload command
func extractActionFromCommand(cmd cpln.Command) string {
	overrides, ok := cmd.Spec["containerOverrides"].([]interface{})
	if !ok {
		return ""
	}
	for _, override := range overrides {
		co, ok := override.(map[string]interface{})
		if !ok {
			continue
		}
		env, ok := co["env"].([]interface{})
		if !ok {
			continue
		}
		for _, e := range env {
			ev, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			if ev["name"] == "ACTION" {
				if val, ok := ev["value"].(string); ok {
					return val
				}
			}
		}
	}
	return ""
}

// extractDatasetFromCommand extracts the DATASET env var from a runCronWorkload command
func extractDatasetFromCommand(cmd cpln.Command) string {
	return extractEnvFromCommand(cmd, "DATASET")
}

// extractTypeFromCommand extracts the TYPE env var from a runCronWorkload command
func extractTypeFromCommand(cmd cpln.Command) string {
	return extractEnvFromCommand(cmd, "TYPE")
}

// extractEnvFromCommand extracts a named env var from a runCronWorkload command's containerOverrides
func extractEnvFromCommand(cmd cpln.Command, envName string) string {
	overrides, ok := cmd.Spec["containerOverrides"].([]interface{})
	if !ok {
		return ""
	}
	for _, override := range overrides {
		co, ok := override.(map[string]interface{})
		if !ok {
			continue
		}
		env, ok := co["env"].([]interface{})
		if !ok {
			continue
		}
		for _, e := range env {
			ev, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			if ev["name"] == envName {
				if val, ok := ev["value"].(string); ok {
					return val
				}
			}
		}
	}
	return ""
}

// ImportStatus represents the status of an import operation
type ImportStatus struct {
	TableName      string `json:"tableName"`
	CommandID      string `json:"commandId"`
	LifecycleStage string `json:"lifecycleStage"` // pending, running, completed, failed
}

// ImportsResponse represents the response for /api/imports
type ImportsResponse struct {
	Imports []ImportStatus `json:"imports"`
}

// handleImports handles GET /api/imports - returns active import commands
func (s *Server) handleImports(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Check CPLN client is configured
	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	orchestratorWorkload := s.getOrchestratorWorkload()

	// Query for active commands (pending or running)
	commands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, orchestratorWorkload, 0)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to query commands: %v", err))
		return
	}

	var imports []ImportStatus
	for _, cmd := range commands.Items {
		if cmd.Type == "runCronWorkload" {
			// Check if this is an import command (ACTION=import)
			action := extractActionFromCommand(cmd)
			if action != "import" {
				continue
			}

			tableName := extractTableNameFromCommand(cmd)
			if tableName == "" {
				continue
			}

			imports = append(imports, ImportStatus{
				TableName:      tableName,
				CommandID:      cmd.ID,
				LifecycleStage: cmd.LifecycleStage,
			})
		}
	}

	response := ImportsResponse{
		Imports: imports,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// RepairStatus represents the status of a repair operation
type RepairStatus struct {
	CommandID      string `json:"commandId"`
	SourceReplica  *int   `json:"sourceReplica,omitempty"`
	LifecycleStage string `json:"lifecycleStage"` // pending, running, completed, failed
}

// RepairsResponse represents the response for /api/repairs
type RepairsResponse struct {
	Repairs []RepairStatus `json:"repairs"`
}

// handleRepairs handles GET /api/repairs - returns active repair commands
func (s *Server) handleRepairs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Check CPLN client is configured
	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	orchestratorWorkload := s.getOrchestratorWorkload()

	// Query for active commands (pending or running)
	commands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, orchestratorWorkload, 0)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to query commands: %v", err))
		return
	}

	var repairs []RepairStatus
	for _, cmd := range commands.Items {
		if cmd.Type == "runCronWorkload" {
			// Check if this is a repair command (ACTION=repair)
			action := extractActionFromCommand(cmd)
			if action != "repair" {
				continue
			}

			sourceReplica := extractSourceReplicaFromCommand(cmd)
			repairs = append(repairs, RepairStatus{
				CommandID:      cmd.ID,
				SourceReplica:  sourceReplica,
				LifecycleStage: cmd.LifecycleStage,
			})
		}
	}

	response := RepairsResponse{
		Repairs: repairs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// extractSourceReplicaFromCommand extracts the REPAIR_SOURCE_REPLICA env var from a runCronWorkload command
func extractSourceReplicaFromCommand(cmd cpln.Command) *int {
	overrides, ok := cmd.Spec["containerOverrides"].([]interface{})
	if !ok {
		return nil
	}
	for _, override := range overrides {
		co, ok := override.(map[string]interface{})
		if !ok {
			continue
		}
		env, ok := co["env"].([]interface{})
		if !ok {
			continue
		}
		for _, e := range env {
			ev, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			if ev["name"] == "REPAIR_SOURCE_REPLICA" {
				if val, ok := ev["value"].(string); ok {
					if i, err := strconv.Atoi(val); err == nil {
						return &i
					}
				}
			}
		}
	}
	return nil
}

// CommandHistoryEntry represents a command in the history
type CommandHistoryEntry struct {
	ID             string `json:"id"`
	Action         string `json:"action"`                  // "import", "repair", "backup", or "restore"
	TableName      string `json:"tableName,omitempty"`     // for imports, backups, and restores
	Type           string `json:"type,omitempty"`          // "delta" or "main" for backups and restores
	Filename       string `json:"filename,omitempty"`      // restore filename for retry
	SourceReplica  *int   `json:"sourceReplica,omitempty"` // only for repairs
	LifecycleStage string `json:"lifecycleStage"`
	Created        string `json:"created"` // ISO timestamp for sorting
}

// CommandHistoryResponse represents the response for /api/commands
type CommandHistoryResponse struct {
	Commands []CommandHistoryEntry `json:"commands"`
}

// handleCommands handles GET /api/commands - returns command history
func (s *Server) handleCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Check CPLN client is configured
	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	orchestratorWorkload := s.getOrchestratorWorkload()

	// Query all commands (all lifecycle stages), limit to 50 for history display
	commands, err := s.cplnClient.QueryAllCommands(s.config.GVC, orchestratorWorkload, 50)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to query commands: %v", err))
		return
	}

	var history []CommandHistoryEntry
	for _, cmd := range commands.Items {
		action := extractActionFromCommand(cmd)

		// Include both import and repair commands
		if action == "import" {
			tableName := extractTableNameFromCommand(cmd)
			if tableName == "" {
				continue
			}
			history = append(history, CommandHistoryEntry{
				ID:             cmd.ID,
				Action:         "import",
				TableName:      tableName,
				LifecycleStage: cmd.LifecycleStage,
				Created:        cmd.Created,
			})
		} else if action == "repair" {
			sourceReplica := extractSourceReplicaFromCommand(cmd)
			history = append(history, CommandHistoryEntry{
				ID:             cmd.ID,
				Action:         "repair",
				SourceReplica:  sourceReplica,
				LifecycleStage: cmd.LifecycleStage,
				Created:        cmd.Created,
			})
		}
	}

	// Also query backup workload for backup command history
	backupWorkload := s.getBackupWorkload()
	backupCommands, backupErr := s.cplnClient.QueryAllCommands(s.config.GVC, backupWorkload, 50)
	if backupErr != nil {
		slog.Warn("failed to query backup command history", "error", backupErr)
	} else {
		for _, cmd := range backupCommands.Items {
			action := extractActionFromCommand(cmd)
			if action == "backup" || action == "restore" {
				dataset := extractDatasetFromCommand(cmd)
				if dataset == "" {
					continue
				}
				backupType := extractTypeFromCommand(cmd)
				entry := CommandHistoryEntry{
					ID:             cmd.ID,
					Action:         action,
					TableName:      dataset,
					Type:           backupType,
					LifecycleStage: cmd.LifecycleStage,
					Created:        cmd.Created,
				}
				if action == "restore" {
					entry.Filename = extractEnvFromCommand(cmd, "RESTORE_FILE")
				}
				history = append(history, entry)
			}
		}
	}

	// Sort merged history by Created timestamp (descending)
	sort.Slice(history, func(i, j int) bool {
		return history[i].Created > history[j].Created
	})

	// Trim to 50 entries
	if len(history) > 50 {
		history = history[:50]
	}

	response := CommandHistoryResponse{
		Commands: history,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleCommandRetry handles POST /api/commands/retry - retries a failed command
func (s *Server) handleCommandRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	var req struct {
		CommandID string `json:"commandId"`
		Workload  string `json:"workload"` // "orchestrator" or "backup"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.CommandID == "" || req.Workload == "" {
		jsonError(w, http.StatusBadRequest, "commandId and workload are required")
		return
	}

	// Determine which CPLN workload to query
	var workloadName string
	switch req.Workload {
	case "orchestrator":
		workloadName = s.getOrchestratorWorkload()
	case "backup":
		workloadName = s.getBackupWorkload()
	default:
		jsonError(w, http.StatusBadRequest, "workload must be 'orchestrator' or 'backup'")
		return
	}

	// Find the original command to extract its parameters
	commands, err := s.cplnClient.QueryAllCommands(s.config.GVC, workloadName, 50)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to query commands: %v", err))
		return
	}

	var originalCmd *cpln.Command
	for _, cmd := range commands.Items {
		if cmd.ID == req.CommandID {
			originalCmd = &cmd
			break
		}
	}
	if originalCmd == nil {
		jsonError(w, http.StatusNotFound, fmt.Sprintf("command %s not found", req.CommandID))
		return
	}

	if originalCmd.LifecycleStage != "failed" {
		jsonError(w, http.StatusBadRequest, fmt.Sprintf("can only retry failed commands (current stage: %s)", originalCmd.LifecycleStage))
		return
	}

	// Extract action and parameters from the original command
	action := extractActionFromCommand(*originalCmd)

	switch action {
	case "import":
		tableName := extractTableNameFromCommand(*originalCmd)
		if tableName == "" {
			jsonError(w, http.StatusBadRequest, "could not extract tableName from original command")
			return
		}
		// Build a synthetic request and delegate to handleImport's logic
		importBody, _ := json.Marshal(map[string]string{"tableName": tableName})
		syntheticReq, _ := http.NewRequest("POST", "/api/import", bytes.NewReader(importBody))
		syntheticReq.Header.Set("Content-Type", "application/json")
		syntheticReq.Header.Set("Authorization", r.Header.Get("Authorization"))
		s.handleImport(w, syntheticReq)

	case "backup":
		tableName := extractDatasetFromCommand(*originalCmd)
		backupType := extractTypeFromCommand(*originalCmd)
		if tableName == "" || backupType == "" {
			jsonError(w, http.StatusBadRequest, "could not extract tableName/type from original command")
			return
		}
		importBody, _ := json.Marshal(map[string]string{"tableName": tableName, "type": backupType})
		syntheticReq, _ := http.NewRequest("POST", "/api/backup", bytes.NewReader(importBody))
		syntheticReq.Header.Set("Content-Type", "application/json")
		syntheticReq.Header.Set("Authorization", r.Header.Get("Authorization"))
		s.handleBackup(w, syntheticReq)

	case "restore":
		tableName := extractDatasetFromCommand(*originalCmd)
		restoreType := extractTypeFromCommand(*originalCmd)
		filename := extractEnvFromCommand(*originalCmd, "RESTORE_FILE")
		if tableName == "" || filename == "" {
			jsonError(w, http.StatusBadRequest, "could not extract tableName/filename from original command")
			return
		}
		if restoreType == "" {
			restoreType = "delta"
		}
		importBody, _ := json.Marshal(map[string]interface{}{
			"tableName": tableName,
			"filename":  filename,
			"type":      restoreType,
		})
		syntheticReq, _ := http.NewRequest("POST", "/api/restore", bytes.NewReader(importBody))
		syntheticReq.Header.Set("Content-Type", "application/json")
		syntheticReq.Header.Set("Authorization", r.Header.Get("Authorization"))
		s.handleRestore(w, syntheticReq)

	default:
		jsonError(w, http.StatusBadRequest, fmt.Sprintf("unsupported action for retry: %s", action))
	}
}

// handleImport handles POST /api/import - triggers cron workload for import
func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Check CPLN client is configured
	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	// Parse request body
	var req struct {
		TableName string `json:"tableName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.TableName == "" {
		jsonError(w, http.StatusBadRequest, "tableName is required")
		return
	}

	// Validate table exists in config
	if _, err := getCSVPathForTable(s.config.TablesConfig, req.TableName); err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	orchestratorWorkload := s.getOrchestratorWorkload()

	// Check for in-progress imports or repairs (pending or running) in a single query
	commands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, orchestratorWorkload, 0)
	if err != nil {
		slog.Warn("failed to query active commands", "error", err)
		// Continue anyway - better to allow potential duplicate than block all imports
	} else {
		for _, cmd := range commands.Items {
			if cmd.Type == "runCronWorkload" {
				action := extractActionFromCommand(cmd)
				// Check if this is an import for the same table
				if action == "import" {
					tableName := extractTableNameFromCommand(cmd)
					if tableName == req.TableName {
						jsonError(w, http.StatusConflict,
							fmt.Sprintf("import already in progress for table %s (command %s)", req.TableName, cmd.ID))
						return
					}
				}
				// Block import if repair is in progress
				if action == "repair" {
					jsonError(w, http.StatusConflict,
						fmt.Sprintf("repair in progress (command %s), cannot import", cmd.ID))
					return
				}
			}
		}
	}

	// Start the cron workload with table-specific overrides
	overrides := []cpln.ContainerOverride{
		{
			Name: "orchestrator",
			Env: []cpln.EnvVar{
				{Name: "ACTION", Value: "import"},
				{Name: "TABLE_NAME", Value: req.TableName},
				{Name: "IMPORT_POLL_INTERVAL", Value: os.Getenv("IMPORT_POLL_INTERVAL")},
				{Name: "IMPORT_POLL_TIMEOUT", Value: os.Getenv("IMPORT_POLL_TIMEOUT")},
			},
		},
	}

	slog.Info("triggering import via cron workload", "table", req.TableName, "workload", orchestratorWorkload)
	cmd, err := s.cplnClient.StartCronWorkload(s.config.GVC, orchestratorWorkload, s.config.Location, overrides)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to start import: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "accepted",
		"message":   fmt.Sprintf("import started for table %s", req.TableName),
		"commandId": cmd.ID,
	})
}

// triggerBackup validates and triggers a backup cron workload for the given table and type.
// Returns the command ID on success, or an error describing what went wrong.
func (s *Server) triggerBackup(tableName, backupType string) (string, error) {
	if s.cplnClient == nil {
		return "", fmt.Errorf("CPLN API client not configured")
	}

	if tableName == "" {
		return "", fmt.Errorf("tableName is required")
	}
	if backupType != "delta" && backupType != "main" {
		return "", fmt.Errorf("type must be 'delta' or 'main'")
	}

	// Validate table exists in config
	if _, err := getCSVPathForTable(s.config.TablesConfig, tableName); err != nil {
		return "", fmt.Errorf("invalid table: %w", err)
	}

	backupWorkload := s.getBackupWorkload()
	orchestratorWorkload := s.getOrchestratorWorkload()

	// Check backup workload for duplicate backup of same table
	backupCommands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, backupWorkload, 0)
	if err != nil {
		slog.Warn("failed to query active backup commands", "error", err)
	} else {
		for _, cmd := range backupCommands.Items {
			if cmd.Type == "runCronWorkload" {
				action := extractActionFromCommand(cmd)
				if action == "backup" {
					dataset := extractDatasetFromCommand(cmd)
					if dataset == tableName {
						return "", fmt.Errorf("backup already in progress for table %s (command %s)", tableName, cmd.ID)
					}
				}
			}
		}
	}

	// Check orchestrator workload for active repairs
	orchCommands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, orchestratorWorkload, 0)
	if err != nil {
		slog.Warn("failed to query active orchestrator commands", "error", err)
	} else {
		for _, cmd := range orchCommands.Items {
			if cmd.Type == "runCronWorkload" {
				action := extractActionFromCommand(cmd)
				if action == "repair" {
					return "", fmt.Errorf("repair in progress (command %s), cannot start backup", cmd.ID)
				}
			}
		}
	}

	// Build env vars for cron workload
	envVars := []cpln.EnvVar{
		{Name: "ACTION", Value: "backup"},
		{Name: "DATASET", Value: tableName},
		{Name: "TYPE", Value: backupType},
	}

	// For main table backup, discover the active slot
	if backupType == "main" {
		replicaCount, err := s.getReplicaCount()
		if err != nil {
			return "", fmt.Errorf("failed to get replica count: %v", err)
		}
		clients := s.buildClients(replicaCount)
		slot := actions2.DiscoverTableSlot(clients, tableName)
		if slot == "" {
			return "", fmt.Errorf("cannot determine active main table slot for %s — ensure at least one healthy replica is available", tableName)
		}
		envVars = append(envVars, cpln.EnvVar{Name: "SLOT", Value: slot})
		slog.Info("discovered active slot for main backup", "table", tableName, "slot", slot)
	}

	// Start the backup cron workload with overrides
	overrides := []cpln.ContainerOverride{
		{
			Name: s.config.BackupContainer,
			Env:  envVars,
		},
	}

	slog.Info("triggering backup via cron workload", "table", tableName, "type", backupType, "workload", backupWorkload)
	cmd, err := s.cplnClient.StartCronWorkload(s.config.GVC, backupWorkload, s.config.Location, overrides)
	if err != nil {
		return "", fmt.Errorf("failed to start backup: %v", err)
	}

	return cmd.ID, nil
}

// handleBackup handles POST /api/backup - triggers cron workload for backup
func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TableName string `json:"tableName"`
		Type      string `json:"type"` // "delta" (default) or "main"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Type == "" {
		req.Type = "delta"
	}

	cmdID, err := s.triggerBackup(req.TableName, req.Type)
	if err != nil {
		// Map specific errors to appropriate HTTP status codes
		errMsg := err.Error()
		switch {
		case strings.Contains(errMsg, "not configured"):
			jsonError(w, http.StatusServiceUnavailable, errMsg)
		case strings.Contains(errMsg, "is required") || strings.Contains(errMsg, "must be") || strings.Contains(errMsg, "invalid table") || strings.Contains(errMsg, "cannot determine"):
			jsonError(w, http.StatusBadRequest, errMsg)
		case strings.Contains(errMsg, "already in progress") || strings.Contains(errMsg, "repair in progress"):
			jsonError(w, http.StatusConflict, errMsg)
		default:
			jsonError(w, http.StatusInternalServerError, errMsg)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "accepted",
		"message":   fmt.Sprintf("%s backup started for table %s", req.Type, req.TableName),
		"commandId": cmdID,
	})
}

// BackupStatus represents the status of a backup operation
type BackupStatus struct {
	TableName      string `json:"tableName"`
	CommandID      string `json:"commandId"`
	LifecycleStage string `json:"lifecycleStage"`
}

// BackupsResponse represents the response for /api/backups
type BackupsResponse struct {
	Backups []BackupStatus `json:"backups"`
}

// handleBackups handles GET /api/backups - returns active backup commands
func (s *Server) handleBackups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	backupWorkload := s.getBackupWorkload()

	commands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, backupWorkload, 0)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to query commands: %v", err))
		return
	}

	var backups []BackupStatus
	for _, cmd := range commands.Items {
		if cmd.Type == "runCronWorkload" {
			action := extractActionFromCommand(cmd)
			if action != "backup" {
				continue
			}
			dataset := extractDatasetFromCommand(cmd)
			if dataset == "" {
				continue
			}
			backups = append(backups, BackupStatus{
				TableName:      dataset,
				CommandID:      cmd.ID,
				LifecycleStage: cmd.LifecycleStage,
			})
		}
	}

	response := BackupsResponse{
		Backups: backups,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// BackupFilesResponse represents the response for /api/backups/files
type BackupFilesResponse struct {
	Backups []s3.BackupFile `json:"backups"`
}

// handleBackupFiles handles GET /api/backups/files - returns list of backup files for a table
func (s *Server) handleBackupFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Check backup client is configured
	if s.backupClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "backup storage not configured (BACKUP_BUCKET/BACKUP_PROVIDER required)")
		return
	}

	// Get table name from query param
	tableName := r.URL.Query().Get("tableName")
	if tableName == "" {
		jsonError(w, http.StatusBadRequest, "tableName query parameter is required")
		return
	}

	// Validate table exists in config
	if _, err := getCSVPathForTable(s.config.TablesConfig, tableName); err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Get backup type from query param (default to delta)
	backupType := r.URL.Query().Get("type")
	if backupType == "" {
		backupType = "delta"
	}
	if backupType != "delta" && backupType != "main" {
		jsonError(w, http.StatusBadRequest, "type must be 'delta' or 'main'")
		return
	}

	// List backups from storage
	backups, err := s.backupClient.ListBackups(r.Context(), s.config.BackupPrefix, tableName, backupType)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list backups: %v", err))
		return
	}

	response := BackupFilesResponse{
		Backups: backups,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleRestore handles POST /api/restore - triggers cron workload for restore
func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Check CPLN client is configured
	if s.cplnClient == nil {
		jsonError(w, http.StatusServiceUnavailable, "CPLN API client not configured")
		return
	}

	// Parse request body
	var req struct {
		TableName string `json:"tableName"`
		Filename  string `json:"filename"`
		Type      string `json:"type"` // "delta" (default) or "main"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.TableName == "" {
		jsonError(w, http.StatusBadRequest, "tableName is required")
		return
	}
	if req.Filename == "" {
		jsonError(w, http.StatusBadRequest, "filename is required")
		return
	}
	if req.Type == "" {
		req.Type = "delta"
	}
	if req.Type != "delta" && req.Type != "main" {
		jsonError(w, http.StatusBadRequest, "type must be 'delta' or 'main'")
		return
	}

	// Validate table exists in config
	if _, err := getCSVPathForTable(s.config.TablesConfig, req.TableName); err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	backupWorkload := s.getBackupWorkload()
	orchestratorWorkload := s.getOrchestratorWorkload()

	// Check backup workload for active operations
	backupCommands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, backupWorkload, 0)
	if err != nil {
		slog.Warn("failed to query active backup commands", "error", err)
	} else {
		for _, cmd := range backupCommands.Items {
			if cmd.Type == "runCronWorkload" {
				action := extractActionFromCommand(cmd)
				if action == "backup" || action == "restore" {
					jsonError(w, http.StatusConflict,
						fmt.Sprintf("%s operation in progress (command %s), cannot start restore", action, cmd.ID))
					return
				}
			}
		}
	}

	// Check orchestrator workload for active repairs
	orchCommands, err := s.cplnClient.QueryActiveCommands(s.config.GVC, orchestratorWorkload, 0)
	if err != nil {
		slog.Warn("failed to query active orchestrator commands", "error", err)
	} else {
		for _, cmd := range orchCommands.Items {
			if cmd.Type == "runCronWorkload" {
				action := extractActionFromCommand(cmd)
				if action == "repair" {
					jsonError(w, http.StatusConflict,
						fmt.Sprintf("repair in progress (command %s), cannot start restore", cmd.ID))
					return
				}
			}
		}
	}

	// Build env vars for cron workload
	envVars := []cpln.EnvVar{
		{Name: "ACTION", Value: "restore"},
		{Name: "DATASET", Value: req.TableName},
		{Name: "TYPE", Value: req.Type},
		{Name: "RESTORE_FILE", Value: req.Filename},
	}

	// For main table restore, use blue-green: discover active slot, target inactive
	if req.Type == "main" {
		// Parse backup slot from filename (the slot the backup was taken from)
		// Filename format: {dataset}_main_{slot}-{timestamp}.tar.gz
		prefix := req.TableName + "_main_"
		idx := strings.Index(req.Filename, prefix)
		if idx == -1 || len(req.Filename) < idx+len(prefix)+1 {
			jsonError(w, http.StatusBadRequest,
				fmt.Sprintf("cannot determine slot from restore filename: %s", req.Filename))
			return
		}
		backupSlot := string(req.Filename[idx+len(prefix)])
		if backupSlot != "a" && backupSlot != "b" {
			jsonError(w, http.StatusBadRequest,
				fmt.Sprintf("invalid slot '%s' in restore filename: %s", backupSlot, req.Filename))
			return
		}

		// Discover active slot and compute inactive target
		replicaCount, err := s.getReplicaCount()
		if err != nil {
			jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
			return
		}
		clients := s.buildClients(replicaCount)
		activeSlot := actions2.DiscoverTableSlot(clients, req.TableName)

		var targetSlot string
		if activeSlot == "a" {
			targetSlot = "b"
		} else {
			targetSlot = "a"
		}

		// Derive orchestrator API server URL for cron callback
		// CPLN internal DNS: {workload}.{gvc}.cpln.local:{port}
		orchestratorAPIName := strings.TrimSuffix(s.config.WorkloadName, "-manticore") + "-orchestrator-api"
		orchestratorURL := fmt.Sprintf("http://%s.%s.cpln.local:8080", orchestratorAPIName, s.config.GVC)

		envVars = append(envVars,
			cpln.EnvVar{Name: "SLOT", Value: targetSlot},
			cpln.EnvVar{Name: "BACKUP_SLOT", Value: backupSlot},
			cpln.EnvVar{Name: "ORCHESTRATOR_API_URL", Value: orchestratorURL},
		)
		slog.Info("blue-green restore plan",
			"table", req.TableName,
			"backupSlot", backupSlot,
			"activeSlot", activeSlot,
			"targetSlot", targetSlot,
			"orchestratorURL", orchestratorURL,
			"filename", req.Filename)
	}

	// Start the backup cron workload with restore action
	overrides := []cpln.ContainerOverride{
		{
			Name: s.config.BackupContainer,
			Env:  envVars,
		},
	}

	slog.Info("triggering restore via cron workload", "table", req.TableName, "type", req.Type, "filename", req.Filename, "workload", backupWorkload)
	cmd, err := s.cplnClient.StartCronWorkload(s.config.GVC, backupWorkload, s.config.Location, overrides)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to start restore: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "accepted",
		"message":   fmt.Sprintf("%s restore started for table %s from %s", req.Type, req.TableName, req.Filename),
		"commandId": cmd.ID,
	})
}

// handleRotateMain handles POST /api/rotate-main - rotates distributed table to a new main slot
// This is called by the backup cron after a blue-green restore completes on the agent
func (s *Server) handleRotateMain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TableName string `json:"tableName"`
		NewSlot   string `json:"newSlot"`
		OldSlot   string `json:"oldSlot"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.TableName == "" || req.NewSlot == "" || req.OldSlot == "" {
		jsonError(w, http.StatusBadRequest, "tableName, newSlot, and oldSlot are required")
		return
	}
	if (req.NewSlot != "a" && req.NewSlot != "b") || (req.OldSlot != "a" && req.OldSlot != "b") {
		jsonError(w, http.StatusBadRequest, "newSlot and oldSlot must be 'a' or 'b'")
		return
	}

	slog.Info("starting main table rotation",
		"table", req.TableName, "newSlot", req.NewSlot, "oldSlot", req.OldSlot)

	// Build clients
	slog.Info("rotation: fetching replica count")
	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}
	slog.Info("rotation: building clients", "replicaCount", replicaCount)
	clients := s.buildClients(replicaCount)
	primary := clients[0]

	// Get table config for HA settings
	slog.Info("rotation: fetching table config", "table", req.TableName)
	tableConfig, err := primary.GetTableConfig(req.TableName, 1)
	if err != nil {
		slog.Warn("failed to fetch table config, using defaults", "table", req.TableName, "error", err)
		tableConfig = &client.TableConfigResponse{
			ClusterMain:     true,
			HAStrategy:      "nodeads",
			AgentRetryCount: 0,
		}
	}
	slog.Info("rotation: table config ready", "clusterMain", tableConfig.ClusterMain, "haStrategy", tableConfig.HAStrategy)

	newMainTable := req.TableName + "_main_" + req.NewSlot
	oldMainTable := req.TableName + "_main_" + req.OldSlot
	deltaTable := req.TableName + "_delta"
	distTable := req.TableName

	// Probe which replicas are reachable (some may not be scaled up).
	// Only reachable replicas are included in the agent list and receive SQL commands.
	// Unreachable replicas are skipped — they'll sync via cluster replication when they come online.
	var reachableClients []*client.AgentClient
	var reachableIndices []int
	for i, c := range clients {
		_, err := c.ListTables(1)
		if err != nil {
			slog.Warn("rotation: replica unreachable, skipping", "replica", i, "error", err)
			continue
		}
		reachableClients = append(reachableClients, c)
		reachableIndices = append(reachableIndices, i)
	}
	if len(reachableClients) == 0 {
		jsonError(w, http.StatusInternalServerError, "no reachable replicas found")
		return
	}
	slog.Info("rotation: reachable replicas", "reachable", reachableIndices, "total", replicaCount)

	// Use first reachable client for cluster operations
	primary = reachableClients[0]

	// Build agent addresses from ALL replicas (maxScale), not just reachable ones.
	// Manticore handles health checking on mirrors via ha_strategy.
	locals := []string{newMainTable, deltaTable}
	var agents []string
	for _, c := range clients {
		agentAddr := extractAgentAddrFromClient(c.BaseURL())
		if agentAddr != "" {
			agents = append(agents, agentAddr)
		}
	}

	// ALTER distributed table on each reachable replica
	slog.Info("swapping distributed table on reachable replicas", "table", distTable, "locals", locals, "agents", agents)
	alterErrors := 0
	for j, c := range reachableClients {
		if err := c.AlterDistributed(distTable, locals, agents, tableConfig.HAStrategy, tableConfig.AgentRetryCount, 1); err != nil {
			slog.Error("failed to alter distributed table on replica", "replica", reachableIndices[j], "error", err)
			alterErrors++
			continue
		}
		slog.Info("swapped distributed table", "replica", reachableIndices[j])
	}
	if alterErrors == len(reachableClients) {
		jsonError(w, http.StatusInternalServerError, "failed to alter distributed table on all reachable replicas")
		return
	}

	// Remove old table from cluster, then drop on reachable replicas
	if tableConfig.ClusterMain {
		slog.Info("removing old main table from cluster", "table", oldMainTable)
		if err := primary.ClusterDrop(oldMainTable, 1); err != nil {
			slog.Warn("failed to remove old table from cluster", "table", oldMainTable, "error", err)
		}
	}

	slog.Info("dropping old main table on reachable replicas", "table", oldMainTable)
	for j, c := range reachableClients {
		if err := c.DropTable(oldMainTable, 1); err != nil {
			slog.Warn("failed to drop old table", "table", oldMainTable, "replica", reachableIndices[j], "error", err)
		}
	}

	slog.Info("main table rotation completed",
		"table", req.TableName, "newSlot", req.NewSlot, "oldSlot", req.OldSlot,
		"reachableReplicas", len(reachableClients), "totalReplicas", replicaCount)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":            "ok",
		"message":           fmt.Sprintf("rotated %s to slot %s", req.TableName, req.NewSlot),
		"reachableReplicas": len(reachableClients),
		"totalReplicas":     replicaCount,
	})
}

// extractAgentAddrFromClient converts an HTTP base URL to a Manticore agent address (hostname:9306)
func extractAgentAddrFromClient(httpURL string) string {
	u, err := url.Parse(httpURL)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s:9306", u.Hostname())
}

// runCLI runs the orchestrator in CLI mode (for cron jobs)
func runCLI(config Config) {
	action := getEnv("ACTION", "health")
	tableName := getEnv("TABLE_NAME", "")
	replicaIndex := getEnvInt("REPLICA_INDEX", 0)
	repairSourceReplicaStr := getEnv("REPAIR_SOURCE_REPLICA", "")
	var repairSourceReplica int = -1
	if repairSourceReplicaStr != "" {
		repairSourceReplica, _ = strconv.Atoi(repairSourceReplicaStr)
	}

	// Set up signal handling for graceful shutdown (important for imports)
	goCtx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		slog.Info("received signal, initiating graceful shutdown", "signal", sig)
		cancel()
	}()

	// For CLI mode, we still need REPLICA_COUNT since we may not have cpln access
	replicaCount := getEnvInt("REPLICA_COUNT", 2)

	// TABLE_NAME is only required for import action
	var csvPath string
	if action == "import" {
		if tableName == "" {
			slog.Error("TABLE_NAME environment variable is required for import action")
			os.Exit(1)
		}
		var err error
		csvPath, err = getCSVPathForTable(config.TablesConfig, tableName)
		if err != nil {
			slog.Error("failed to get CSV path", "table", tableName, "error", err)
			os.Exit(1)
		}
	}

	slog.Info("Manticore Orchestrator starting (CLI mode)", "action", action, "replicas", replicaCount, "workload", config.WorkloadName)

	clients := buildClientsStatic(config, replicaCount)

	// Initialize S3 client and indexer builder if S3 bucket is configured (for indexer method)
	// Initialize S3 client if S3 bucket is configured
	var s3Client *s3.Client
	if config.S3Bucket != "" {
		var err error
		s3Client, err = s3.NewClient(config.S3Bucket, config.S3Region)
		if err != nil {
			slog.Warn("failed to create S3 client", "error", err)
		} else {
			slog.Debug("S3 client initialized", "bucket", config.S3Bucket)
		}
	}

	// Initialize indexer builder if either S3 or shared volume is configured
	var indexerBuilder *indexer.IndexBuilder
	if s3Client != nil || config.SharedVolumeMount != "" {
		indexerBuilder = indexer.NewIndexBuilder(config.IndexerWorkDir)
		if config.SharedVolumeMount != "" {
			slog.Debug("indexer builder initialized with shared volume", "sharedVolume", config.SharedVolumeMount)
		} else {
			slog.Debug("indexer builder initialized with S3", "workDir", config.IndexerWorkDir)
		}
	}

	ctx := &actions2.Context{
		Clients:           clients,
		Dataset:           tableName,
		CSVPath:           csvPath,
		S3Client:          s3Client,
		IndexerBuilder:    indexerBuilder,
		S3IndexPrefix:     config.S3IndexPrefix,
		S3Mount:           config.S3Mount,
		IndexerWorkDir:    config.IndexerWorkDir,
		ImportMemLimit:    config.IndexerMemLimit,
		SharedVolumeMount: config.SharedVolumeMount,
	}

	var actionErr error
	switch action {
	case "health":
		actionErr = actions2.Health(ctx)

	case "init":
		// Get ALL tables from TABLES_CONFIG (like HTTP mode)
		tables, err := getTablesConfigFromJSON(config.TablesConfig)
		if err != nil {
			slog.Error("failed to parse TABLES_CONFIG", "error", err)
			os.Exit(1)
		}
		if len(tables) == 0 {
			slog.Error("no tables configured in TABLES_CONFIG")
			os.Exit(1)
		}
		actionTables := make([]actions2.TableConfig, len(tables))
		for i, t := range tables {
			actionTables[i] = actions2.TableConfig{Name: t.Name, CsvPath: t.CsvPath}
		}
		clientBuilder := func(count int) []*client.AgentClient {
			return buildClientsStatic(config, count)
		}
		replicaCountFetcher := func() (int, error) {
			return replicaCount, nil // Static for CLI mode
		}
		// For CLI mode, use a high aliveSinceStart to allow immediate bootstrap if needed
		_, actionErr = actions2.Init(goCtx, clientBuilder, replicaCountFetcher, actionTables, replicaIndex, nil, 999999, 60)

	case "import":
		// Pass context for graceful shutdown - cleanup runs automatically on cancellation
		actionErr = actions2.Import(goCtx, ctx)

	case "repair":
		if repairSourceReplica >= 0 {
			slog.Debug("using explicit source replica for repair", "sourceReplica", repairSourceReplica)
			actionErr = actions2.RepairWithSource(ctx, repairSourceReplica)
		} else {
			slog.Debug("using intelligent source selection")
			actionErr = actions2.Repair(ctx)
		}

	case "config":
		actionErr = runCLIConfig(config, clients)

	case "cluster":
		actionErr = runCLICluster(clients)

	case "discover":
		actionErr = runCLIDiscover(clients, config.WorkloadName)

	case "tables-status":
		actionErr = runCLITablesStatus(config, clients)

	default:
		slog.Error("unknown action", "action", action)
		os.Exit(1)
	}

	if actionErr != nil {
		slog.Error("action failed", "action", action, "error", actionErr)
		os.Exit(1)
	}

	slog.Info("action completed successfully", "action", action)
}

// runCLIConfig outputs cluster configuration as JSON (equivalent to GET /api/config)
func runCLIConfig(config Config, clients []*client.AgentClient) error {
	tables, err := getTablesConfigFromJSON(config.TablesConfig)
	if err != nil {
		return err
	}

	response := map[string]interface{}{
		"replicaCount": len(clients),
		"workloadName": config.WorkloadName,
		"gvc":          config.GVC,
		"location":     config.Location,
		"tables":       tables,
	}

	output, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(output))
	return nil
}

// runCLICluster outputs replica status as JSON (equivalent to GET /api/cluster)
func runCLICluster(clients []*client.AgentClient) error {
	replicas := make([]ReplicaStatus, len(clients))

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(idx int, agentClient *client.AgentClient) {
			defer wg.Done()

			replica := ReplicaStatus{
				Index:    idx,
				Endpoint: agentClient.BaseURL(),
				Status:   "offline",
			}

			health, err := agentClient.Health(0) // Default retries for CLI
			if err != nil {
				status, message := classifyReplicaError(err)
				replica.Status = status
				replica.Error = &message
			} else {
				replica.Status = "online"
				if health.ClusterStatus != "" {
					replica.ClusterStatus = &health.ClusterStatus
				}
			}

			replicas[idx] = replica
		}(i, c)
	}
	wg.Wait()

	// Determine overall status
	status := "uninitialized"
	onlineCount := 0
	inUseCount := 0
	hasCluster := false
	allHealthy := true

	for _, r := range replicas {
		if r.Status != "not_in_use" {
			inUseCount++
		}
		if r.Status == "online" {
			onlineCount++
			if r.ClusterStatus != nil {
				hasCluster = true
				cs := *r.ClusterStatus
				if cs != "primary" && cs != "synced" {
					allHealthy = false
				}
			}
		}
	}

	if hasCluster {
		if onlineCount == inUseCount && allHealthy {
			status = "healthy"
		} else {
			status = "degraded"
		}
	}

	response := ClusterResponse{
		Status:   status,
		Replicas: replicas,
	}

	output, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(output))
	return nil
}

// runCLIDiscover outputs winning cluster info as JSON (equivalent to GET /api/cluster/discover)
func runCLIDiscover(clients []*client.AgentClient, workloadName string) error {
	replicas := make([]cluster.ReplicaInfo, len(clients))
	var wg sync.WaitGroup

	for i, c := range clients {
		wg.Add(1)
		go func(idx int, agentClient *client.AgentClient) {
			defer wg.Done()

			replica := cluster.ReplicaInfo{Index: idx}

			health, err := agentClient.Health(0) // Default retries for CLI
			if err != nil {
				replica.Reachable = false
				replicas[idx] = replica
				return
			}
			replica.Reachable = true
			replica.ClusterStatus = health.ClusterStatus

			gs, err := agentClient.Grastate(0) // Default retries for CLI
			if err != nil {
				replicas[idx] = replica
				return
			}

			replica.UUID = gs.UUID
			replica.Seqno = gs.Seqno
			replica.HasValidUUID = gs.Exists && gs.UUID != "" && gs.UUID != "00000000-0000-0000-0000-000000000000"

			replicas[idx] = replica
		}(i, c)
	}
	wg.Wait()

	desc := cluster.FindWinningCluster(replicas, workloadName, "9312")

	// Find all UUID groups for split-brain detection
	groups := cluster.FindAllClusterGroups(replicas)
	splitBrain := len(groups) > 1

	response := ClusterDiscoverResponse{
		Cluster:    desc,
		SplitBrain: splitBrain,
		Groups:     groups,
	}

	output, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(output))
	return nil
}

// runCLITablesStatus outputs per-replica table status as JSON (equivalent to GET /api/tables/status)
func runCLITablesStatus(config Config, clients []*client.AgentClient) error {
	tablesConfig, err := getTablesConfigFromJSON(config.TablesConfig)
	if err != nil {
		return err
	}

	// Discover active slots by querying agents for existing main tables
	var tableNames []string
	for _, t := range tablesConfig {
		tableNames = append(tableNames, t.Name)
	}
	tableSlots := actions2.DiscoverTableSlots(clients, tableNames)

	// Fetch table configs to get clusterMain setting for each table
	tableConfigs := make(map[string]*client.TableConfigResponse)
	for _, t := range tablesConfig {
		for _, c := range clients {
			cfg, err := c.GetTableConfig(t.Name, 0)
			if err == nil {
				tableConfigs[t.Name] = cfg
				break
			}
		}
	}

	// Fetch tables from each replica
	type replicaTables struct {
		index  int
		tables []types.TableInfo
		err    error
	}
	results := make(chan replicaTables, len(clients))

	for i, c := range clients {
		go func(idx int, agentClient *client.AgentClient) {
			tables, err := agentClient.ListTables(0) // Default retries for CLI
			results <- replicaTables{index: idx, tables: tables, err: err}
		}(i, c)
	}

	replicaTableMap := make(map[int][]types.TableInfo)
	replicaErrors := make(map[int]error)
	for i := 0; i < len(clients); i++ {
		result := <-results
		if result.err != nil {
			replicaErrors[result.index] = result.err
		} else {
			replicaTableMap[result.index] = result.tables
		}
	}

	// Build response
	var tableEntries []TableStatusEntry
	for _, tableConfig := range tablesConfig {
		// Default clusterMain to true if we couldn't fetch config
		clusterMain := true
		if cfg, ok := tableConfigs[tableConfig.Name]; ok {
			clusterMain = cfg.ClusterMain
		}

		entry := TableStatusEntry{
			Name:        tableConfig.Name,
			CsvPath:     tableConfig.CsvPath,
			ClusterMain: clusterMain,
			Replicas:    make([]TableReplicaStatus, len(clients)),
		}

		// Look up the active slot for this specific table (default to "a")
		activeSlot := tableSlots[tableConfig.Name]
		if activeSlot == "" {
			activeSlot = "a"
		}
		mainTableName := fmt.Sprintf("%s_main_%s", tableConfig.Name, activeSlot)
		deltaTableName := fmt.Sprintf("%s_delta", tableConfig.Name)
		distributedTableName := tableConfig.Name

		for idx := 0; idx < len(clients); idx++ {
			replicaStatus := TableReplicaStatus{
				Index:  idx,
				Online: true,
			}

			if err, ok := replicaErrors[idx]; ok {
				replicaStatus.Online = false
				errStr := err.Error()
				replicaStatus.Error = &errStr
			} else if tables, ok := replicaTableMap[idx]; ok {
				for _, t := range tables {
					switch t.Name {
					case mainTableName:
						replicaStatus.MainTable.Present = true
						replicaStatus.MainTable.InCluster = t.InCluster
					case deltaTableName:
						replicaStatus.DeltaTable.Present = true
						replicaStatus.DeltaTable.InCluster = t.InCluster
					case distributedTableName:
						replicaStatus.DistributedTable.Present = true
					}
				}
			}

			entry.Replicas[idx] = replicaStatus
		}

		tableEntries = append(tableEntries, entry)
	}

	response := TablesStatusResponse{
		TableSlots: tableSlots,
		Tables:     tableEntries,
	}

	output, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(output))
	return nil
}

// buildClientsStatic creates agent clients for CLI mode (static replica count)
func buildClientsStatic(config Config, replicaCount int) []*client.AgentClient {
	var clients []*client.AgentClient
	for i := 0; i < replicaCount; i++ {
		endpoint := fmt.Sprintf("http://%s-%d.%s:%s",
			config.WorkloadName, i, config.WorkloadName, config.AgentPort)
		clients = append(clients, client.NewAgentClient(endpoint, config.AuthToken))
	}
	slog.Debug("agent endpoints", "count", len(clients))
	return clients
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// extractName extracts the resource name from a CPLN path like "/org/myorg/location/aws-us-east-2"
// Returns the last path segment, or the original value if no slashes are present
func extractName(path string) string {
	if path == "" {
		return ""
	}
	if idx := strings.LastIndex(path, "/"); idx != -1 {
		return path[idx+1:]
	}
	return path
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getCSVPathForTable(tablesConfigJSON, tableName string) (string, error) {
	var config map[string]string
	if err := json.Unmarshal([]byte(tablesConfigJSON), &config); err != nil {
		return "", fmt.Errorf("failed to parse TABLES_CONFIG: %w", err)
	}

	csvPath, ok := config[tableName]
	if !ok {
		return "", fmt.Errorf("table %s not found in TABLES_CONFIG", tableName)
	}

	return csvPath, nil
}

// getTablesConfigFromJSON parses TABLES_CONFIG JSON and returns all table configs
func getTablesConfigFromJSON(tablesConfigJSON string) ([]TableConfig, error) {
	var configMap map[string]string
	if err := json.Unmarshal([]byte(tablesConfigJSON), &configMap); err != nil {
		return nil, fmt.Errorf("failed to parse TABLES_CONFIG: %w", err)
	}

	var tables []TableConfig
	for name, csvPath := range configMap {
		tables = append(tables, TableConfig{Name: name, CsvPath: csvPath})
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	return tables, nil
}

// handleQuery handles POST /api/query - executes SQL queries against Manticore
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if strings.TrimSpace(req.Query) == "" {
		jsonError(w, http.StatusBadRequest, "query is required")
		return
	}

	// Get replica count
	replicaCount, err := s.getReplicaCount()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get replica count: %v", err))
		return
	}

	if req.Broadcast {
		s.handleBroadcastQuery(w, req.Query, replicaCount)
	} else {
		s.handleSingleQuery(w, req.Query, req.ReplicaIndex, replicaCount)
	}
}

// buildManticoreDSN builds a MySQL DSN for connecting to a Manticore replica
func (s *Server) buildManticoreDSN(replicaIndex int) string {
	host := fmt.Sprintf("%s-%d.%s", s.config.WorkloadName, replicaIndex, s.config.WorkloadName)
	return fmt.Sprintf("tcp(%s:%s)/", host, s.config.ManticoreMySQLPort)
}

// executeQueryOnReplica executes a SQL query on a specific replica and returns the result
func (s *Server) executeQueryOnReplica(query string, replicaIndex int) *ReplicaQueryResult {
	start := time.Now()

	dsn := s.buildManticoreDSN(replicaIndex)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return &ReplicaQueryResult{
			ReplicaIndex: replicaIndex,
			Status:       "error",
			Error:        fmt.Sprintf("connection failed: %v", err),
		}
	}
	defer db.Close()

	// Set connection timeout
	db.SetConnMaxLifetime(30 * time.Second)

	rows, err := db.Query(query)
	if err != nil {
		return &ReplicaQueryResult{
			ReplicaIndex: replicaIndex,
			Status:       "error",
			Error:        err.Error(),
		}
	}
	defer rows.Close()

	// Get column info
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return &ReplicaQueryResult{
			ReplicaIndex: replicaIndex,
			Status:       "error",
			Error:        fmt.Sprintf("failed to get column types: %v", err),
		}
	}

	columns := make([]ColumnMeta, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = ColumnMeta{
			Name: ct.Name(),
			Type: ct.DatabaseTypeName(),
		}
	}

	// Scan rows
	var resultRows []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for readability
			if b, ok := val.([]byte); ok {
				row[col.Name] = string(b)
			} else {
				row[col.Name] = val
			}
		}
		resultRows = append(resultRows, row)
	}

	return &ReplicaQueryResult{
		ReplicaIndex:    replicaIndex,
		Status:          "success",
		Columns:         columns,
		Rows:            resultRows,
		RowCount:        len(resultRows),
		ExecutionTimeMs: time.Since(start).Milliseconds(),
	}
}

// handleSingleQuery handles a query to a single replica (load balanced or targeted)
func (s *Server) handleSingleQuery(w http.ResponseWriter, query string, targetReplica *int, replicaCount int) {
	if targetReplica != nil {
		// Target specific replica
		if *targetReplica < 0 || *targetReplica >= replicaCount {
			jsonError(w, http.StatusBadRequest, fmt.Sprintf("invalid replicaIndex: %d (valid range 0-%d)", *targetReplica, replicaCount-1))
			return
		}

		result := s.executeQueryOnReplica(query, *targetReplica)
		s.writeQueryResponse(w, result)
		return
	}

	// Load balanced: try replicas in order until one succeeds
	for i := 0; i < replicaCount; i++ {
		result := s.executeQueryOnReplica(query, i)
		if result.Status == "success" {
			s.writeQueryResponse(w, result)
			return
		}
		slog.Debug("query failed on replica, trying next", "replica", i, "error", result.Error)
	}

	// All replicas failed
	jsonError(w, http.StatusServiceUnavailable, "query failed on all replicas")
}

// writeQueryResponse writes a ReplicaQueryResult as a QueryResponse
func (s *Server) writeQueryResponse(w http.ResponseWriter, result *ReplicaQueryResult) {
	response := QueryResponse{
		Status:          result.Status,
		Columns:         result.Columns,
		Rows:            result.Rows,
		RowCount:        result.RowCount,
		ExecutionTimeMs: result.ExecutionTimeMs,
		ReplicaIndex:    &result.ReplicaIndex,
		Error:           result.Error,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleBroadcastQuery handles a query broadcast to all replicas
func (s *Server) handleBroadcastQuery(w http.ResponseWriter, query string, replicaCount int) {
	results := make([]ReplicaQueryResult, replicaCount)
	var wg sync.WaitGroup

	for i := 0; i < replicaCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			result := s.executeQueryOnReplica(query, idx)
			results[idx] = *result
		}(i)
	}
	wg.Wait()

	// Filter out not-in-use replicas (DNS errors indicate replica doesn't exist)
	// and determine overall status
	var filteredResults []ReplicaQueryResult
	successCount := 0
	failureCount := 0

	for _, r := range results {
		// Check if this is a "no such host" error (replica doesn't exist)
		if r.Error != "" && strings.Contains(r.Error, "no such host") {
			// Mark as not_in_use and don't count as failure
			r.Status = "not_in_use"
			r.Error = "replica not in use"
		}

		filteredResults = append(filteredResults, r)

		if r.Status == "ok" {
			successCount++
		} else if r.Status != "not_in_use" {
			failureCount++
		}
	}

	// Determine overall status
	overallStatus := "ok"
	if failureCount > 0 && successCount == 0 {
		overallStatus = "error"
	} else if failureCount > 0 {
		overallStatus = "partial"
	}

	response := BroadcastQueryResponse{
		Status:  overallStatus,
		Results: filteredResults,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func jsonError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func jsonSuccess(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": message})
}
