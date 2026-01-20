package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/agent/handlers"
	"github.com/controlplane-com/manticore-orchestrator/pkg/agent/jobs"
	"github.com/controlplane-com/manticore-orchestrator/pkg/agent/manticore"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
	"github.com/gorilla/mux"
)

// startTime tracks when the agent started, used for bootstrap timeout decisions
var startTime = time.Now()

// authMiddleware validates the bearer token for protected endpoints
func authMiddleware(token string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip auth for health and ready endpoints (used by readiness probes)
			if r.URL.Path == "/api/health" || r.URL.Path == "/api/ready" {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, `{"error":"missing Authorization header"}`, http.StatusUnauthorized)
				return
			}

			// Expect "Bearer <token>"
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
}

var h *handlers.Handler

func main() {
	// Setup structured logging with configurable level
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info"))); err != nil {
		logLevel = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	// Configuration from environment
	mysqlHost := getEnv("MYSQL_HOST", "127.0.0.1")
	mysqlPort := getEnv("MYSQL_PORT", "9306")
	httpPort := getEnv("MANTICORE_HTTP_PORT", "9308")
	schemaFile := getEnv("SCHEMA_FILE", "/etc/manticore/schema.conf")
	clusterName := getEnv("CLUSTER_NAME", "myc")
	s3Mount := getEnv("S3_MOUNT", "/mnt/s3")
	listenAddr := getEnv("LISTEN_ADDR", ":8080")
	authToken := getEnv("AUTH_TOKEN", "")
	orchestratorURL := getEnv("ORCHESTRATOR_API_URL", "") // e.g., "http://orchestrator:8080"
	importBatchSize := getEnvInt("IMPORT_BATCH_SIZE", 10000)
	importWorkers := getEnvInt("IMPORT_WORKERS", 4)
	initDelaySeconds := getEnvInt("INIT_DELAY_SECONDS", 30)

	if authToken == "" {
		slog.Error("AUTH_TOKEN environment variable is required")
		os.Exit(1)
	}

	// Get replica index from hostname (e.g., "workload-name-0" -> 0)
	replicaIndex := getReplicaIndexFromHostname()
	slog.Info("detected replica index", "replicaIndex", replicaIndex)

	// Initialize Manticore client (with retry for startup)
	// Must verify SHOW TABLES works, not just TCP connectivity
	var client *manticore.Client
	var err error
	for i := 0; i < 30; i++ {
		client, err = manticore.NewClient(mysqlHost, mysqlPort, httpPort)
		if err != nil {
			slog.Info("waiting for Manticore connection", "attempt", i+1, "maxAttempts", 30, "error", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Verify Manticore is fully initialized by executing SHOW TABLES
		_, err = client.ListTables()
		if err != nil {
			slog.Info("waiting for Manticore to be fully initialized", "attempt", i+1, "maxAttempts", 30, "error", err)
			client.Close()
			client = nil
			time.Sleep(2 * time.Second)
			continue
		}

		slog.Info("Manticore is ready", "attempt", i+1)
		break
	}
	if client == nil || err != nil {
		slog.Error("failed to connect to Manticore after 30 attempts", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// Load schema registry (multi-table YAML format)
	registry := manticore.NewSchemaRegistry()
	if err := registry.LoadFromFile(schemaFile); err != nil {
		slog.Error("failed to load schema registry", "error", err)
		os.Exit(1)
	}
	slog.Info("loaded schemas for tables", "tables", registry.List())

	// Initialize job manager for async imports
	jobsDir := getEnv("JOBS_DIR", "/var/lib/manticore/.jobs")
	jobManager, err := jobs.NewManager(jobsDir)
	if err != nil {
		slog.Error("failed to initialize job manager", "error", err)
		os.Exit(1)
	}
	defer jobManager.Stop()

	// Create handler with dependencies
	h = handlers.NewHandler(client, registry, clusterName, s3Mount, importBatchSize, importWorkers, jobManager)

	// Setup routes
	r := mux.NewRouter()
	api := r.PathPrefix("/api").Subrouter()

	// Apply auth middleware to all API routes
	api.Use(authMiddleware(authToken))

	// Health and readiness (exempt from auth in middleware)
	api.HandleFunc("/health", h.Health).Methods("GET")
	api.HandleFunc("/ready", h.Ready).Methods("GET")
	api.HandleFunc("/grastate", h.Grastate).Methods("GET")
	api.HandleFunc("/tables", h.ListTables).Methods("GET")
	// Table-specific endpoints (must be before PathPrefix to match first)
	api.PathPrefix("/tables/").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/config") {
			h.GetTableConfig(w, r)
		} else {
			h.GetTableSchema(w, r)
		}
	})).Methods("GET")

	// Table operations
	api.HandleFunc("/table/create", h.CreateTableHandler).Methods("POST")
	api.HandleFunc("/table/drop", h.DropTableHandler).Methods("POST")

	// Distributed table operations
	api.HandleFunc("/distributed/alter", h.AlterDistributed).Methods("POST")
	api.HandleFunc("/distributed/create", h.CreateDistributedHandler).Methods("POST")

	// Cluster operations
	api.HandleFunc("/cluster/add", h.ClusterAddHandler).Methods("POST")
	api.HandleFunc("/cluster/drop", h.ClusterDrop).Methods("POST")
	api.HandleFunc("/cluster/status", h.ClusterStatus).Methods("GET")
	api.HandleFunc("/cluster/bootstrap", h.ClusterBootstrapHandler).Methods("POST")
	api.HandleFunc("/cluster/join", h.ClusterJoinHandler).Methods("POST")
	api.HandleFunc("/cluster/rejoin", h.ClusterRejoinHandler).Methods("POST")

	// Import operations (async with polling)
	api.HandleFunc("/import", h.StartImport).Methods("POST")
	api.HandleFunc("/import/{jobId}", h.GetImportStatus).Methods("GET")
	api.HandleFunc("/import/{jobId}", h.CancelImport).Methods("DELETE")

	// Create HTTP server with graceful shutdown support
	server := &http.Server{
		Addr:    listenAddr,
		Handler: r,
	}

	// Start HTTP server (orchestrator needs to call back to it)
	go func() {
		slog.Info("Manticore agent starting HTTP server", "listenAddr", listenAddr)
		slog.Debug("configuration", "mysqlHost", mysqlHost, "mysqlPort", mysqlPort, "cluster", clusterName)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for HTTP server to start, then delay before init to let Manticore cluster manager settle
	// This prevents race condition where Manticore auto-bootstraps during init
	time.Sleep(500 * time.Millisecond) // HTTP server startup
	if orchestratorURL != "" {
		slog.Info("waiting before orchestrator init to let Manticore settle", "delaySeconds", initDelaySeconds)
		time.Sleep(time.Duration(initDelaySeconds) * time.Second)
	}

	// Call orchestrator /init endpoint if configured
	if orchestratorURL != "" {
		agentInit(orchestratorURL, replicaIndex, authToken, client, registry, clusterName)
		slog.Info("initialization complete")
		handlers.SetInitialized(true)
	} else {
		slog.Info("ORCHESTRATOR_URL not set, skipping orchestrator init")
	}

	// Wait for shutdown signal (SIGTERM or SIGINT)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	slog.Info("received shutdown signal, starting graceful shutdown", "signal", sig)

	// Cancel all running import jobs
	cancelled := jobManager.CancelAllJobs()
	slog.Info("cancelled running import jobs", "count", cancelled)

	// Graceful HTTP server shutdown (wait up to 30 seconds for in-flight requests)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	// Cleanup
	jobManager.Stop()
	client.Close()

	slog.Info("shutdown complete")
}

// InitResponse represents the instructions from the orchestrator
type InitResponse struct {
	Action            string            `json:"action"`            // "bootstrap", "join", or "wait"
	RetryAfterSeconds int               `json:"retryAfterSeconds"` // For "wait" action
	TableSlots        map[string]string `json:"tableSlots"`        // table name -> "a" or "b"
	SourceAddr        string            `json:"sourceAddr"`        // For "join" action
	ClusterUUID       string            `json:"clusterUUID"`
}

// agentInit calls the orchestrator's /init endpoint and executes the returned instructions
func agentInit(url string, replicaIdx int, token string, mcClient *manticore.Client, registry *manticore.SchemaRegistry, clusterName string) {
	for {
		retry, err := tryInit(url, replicaIdx, token, mcClient, registry, clusterName)
		if err != nil {
			slog.Error("initialization failed. Retrying in 5 seconds", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if retry {
			slog.Info("Retrying in 5 seconds")
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
}

func tryInit(url string, replicaIndex int, token string, client *manticore.Client, registry *manticore.SchemaRegistry, clusterName string) (bool, error) {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	// Read local grastate.dat to include with request
	grastate := readLocalGrastate()

	// Query local cluster health
	clusterStatus, nodeState, _ := client.GetClusterHealth(clusterName)

	// Build request with replica info, grastate, cluster health, and alive time
	aliveSinceStart := int(time.Since(startTime).Seconds())
	reqBody := map[string]interface{}{
		"replicaIndex":    replicaIndex,
		"aliveSinceStart": aliveSinceStart,
	}
	if grastate != nil {
		reqBody["grastate"] = grastate
	}
	if clusterStatus != "" {
		reqBody["clusterStatus"] = clusterStatus
		reqBody["nodeState"] = nodeState
	}

	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", url+"/api/init", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	slog.Info("calling orchestrator init", "replica", replicaIndex, "aliveSinceStart", aliveSinceStart, "hasGrastate", grastate != nil, "clusterStatus", clusterStatus, "nodeState", nodeState)

	resp, err := httpClient.Do(req)
	if err != nil {
		slog.Warn("failed to call orchestrator", "error", err)
		return false, err
	}

	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Warn("orchestrator returned error", "status", resp.StatusCode, "body", string(respBody))
		return false, fmt.Errorf("orchestrator returned error: %s", respBody)
	}

	var result InitResponse
	if err = json.Unmarshal(respBody, &result); err != nil {
		return false, fmt.Errorf("failed to parse orchestrator response: %w", err)
	}

	slog.Info("orchestrator response", "action", result.Action, "tableSlots", result.TableSlots, "sourceAddr", result.SourceAddr)

	switch result.Action {
	case "continue":
		slog.Info("orchestrator says continue (already initialized)", "tableSlots", result.TableSlots)
		return false, ensureTables(h, registry, result.TableSlots, false)
	case "wait":
		slog.Info("orchestrator says wait before retry", "retryAfterSeconds", result.RetryAfterSeconds)
		time.Sleep(time.Duration(result.RetryAfterSeconds) * time.Second)
		return true, nil
	case "bootstrap":
		slog.Info("orchestrator says bootstrap", "tableSlots", result.TableSlots)
		if err := h.ClusterBootstrap(); err != nil {
			return false, fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
		// Discover slots locally when bootstrapping (API cannot query the caller)
		return false, ensureTables(h, registry, result.TableSlots, true)
	case "join":
		slog.Info("orchestrator says join", "sourceAddr", result.SourceAddr, "tableSlots", result.TableSlots)
		if err := h.ClusterJoin(result.SourceAddr); err != nil {
			return false, fmt.Errorf("failed to join cluster: %w", err)
		}
		return false, ensureTables(h, registry, result.TableSlots, false)
	case "rejoin":
		slog.Info("orchestrator says rejoin", "sourceAddr", result.SourceAddr, "tableSlots", result.TableSlots)
		if err := h.ClusterRejoin(types.ClusterRejoinRequest{SourceAddr: result.SourceAddr}); err != nil {
			return false, fmt.Errorf("failed to rejoin cluster: %w", err)
		}
		return false, ensureTables(h, registry, result.TableSlots, false)
	default:
		return false, fmt.Errorf("unknown action from orchestrator: %s", result.Action)
	}
}

// discoverLocalSlot uses the handler's DiscoverSlot function which checks the distributed table
// first, then falls back to checking which main tables exist
func discoverLocalSlot(handler *handlers.Handler, tableName string) string {
	return handler.DiscoverSlot(tableName)
}

func ensureTables(handler *handlers.Handler, registry *manticore.SchemaRegistry, tableSlots map[string]string, discoverLocally bool) error {
	// Create tables for each schema using per-table slots
	for _, tableName := range registry.List() {
		activeSlot := tableSlots[tableName]
		if activeSlot == "" {
			if discoverLocally {
				activeSlot = discoverLocalSlot(handler, tableName)
				slog.Info("discovered local slot", "table", tableName, "slot", activeSlot)
			}
			// Default to slot "a" if still empty (bootstrap with no existing tables, or join/continue)
			if activeSlot == "" {
				activeSlot = "a"
			}
		}
		otherSlot := "b"
		if activeSlot == "b" {
			otherSlot = "a"
		}

		mainTable := tableName + "_main_" + activeSlot
		otherMainTable := tableName + "_main_" + otherSlot
		deltaTable := tableName + "_delta"

		// Get schema config to determine cluster membership
		schema, ok := registry.Get(tableName)
		clusterMain := true // default
		if ok {
			clusterMain = schema.ClusterMain
		}

		slog.Debug("ensuring tables", "table", tableName, "mainTable", mainTable, "deltaTable", deltaTable, "clusterMain", clusterMain)

		// Drop the other slot's main table if it exists (cleanup from previous slot switch)
		if err := handler.DropTable(otherMainTable); err != nil {
			return fmt.Errorf("failed to drop other slot table: %w", err)
		}

		// Create delta table
		if err := handler.CreateTable(deltaTable); err != nil {
			return fmt.Errorf("failed to create delta table: %w", err)
		}

		// Add delta to cluster (idempotent - handles "already in cluster") - delta is always clustered
		if err := handler.ClusterAdd(deltaTable); err != nil {
			return fmt.Errorf("failed to add delta to cluster: %w", err)
		}

		// Create main table
		if err := handler.CreateTable(mainTable); err != nil {
			return fmt.Errorf("failed to create main table: %w", err)
		}

		// Conditionally add main to cluster based on schema config
		if clusterMain {
			if err := handler.ClusterAdd(mainTable); err != nil {
				return fmt.Errorf("failed to add main to cluster: %w", err)
			}
		} else {
			slog.Debug("skipping cluster add for main table (clusterMain=false)", "table", mainTable)
		}

		// Create distributed table pointing to main and delta (no agents - orchestrator handles mirrors)
		if err := handler.CreateDistributed(tableName, []string{mainTable, deltaTable}, nil, "", 0); err != nil {
			return fmt.Errorf("failed to create distributed table: %w", err)
		}
	}
	return nil
}

// readLocalGrastate reads the local grastate.dat file and returns its contents
func readLocalGrastate() map[string]interface{} {
	path := "/var/lib/manticore/grastate.dat"
	file, err := os.Open(path)
	if err != nil {
		slog.Debug("no grastate.dat found", "path", path, "error", err)
		return nil
	}
	defer file.Close()

	info := map[string]interface{}{"exists": true}
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
			info["uuid"] = value
		case "seqno":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				info["seqno"] = v
			}
		case "safe_to_bootstrap":
			if v, err := strconv.Atoi(value); err == nil {
				info["safeToBootstrap"] = v
			}
		}
	}

	if scanner.Err() != nil {
		slog.Warn("error reading grastate.dat", "error", scanner.Err())
		return nil
	}

	slog.Debug("read grastate.dat", "uuid", info["uuid"], "seqno", info["seqno"])
	return info
}

// getReplicaIndexFromHostname extracts the replica index from the hostname
// Expects format like "workload-name-0" where the last segment is the index
func getReplicaIndexFromHostname() int {
	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		slog.Debug("HOSTNAME not set, defaulting to replica 0")
		return 0
	}

	// Parse from format like "manticore-manticore-0"
	parts := strings.Split(hostname, "-")
	if len(parts) > 0 {
		lastPart := parts[len(parts)-1]
		if idx, err := strconv.Atoi(lastPart); err == nil {
			return idx
		}
	}

	slog.Warn("could not parse replica index from hostname, defaulting to 0", "hostname", hostname)
	return 0
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}
