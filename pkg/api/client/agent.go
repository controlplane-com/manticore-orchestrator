package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/schema"
	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/types"
)

// Restart recovery constants
const (
	RestartMaxConsecutiveFailures = 3                // consecutive poll failures before assuming restart
	RestartRecoveryTimeout        = 5 * time.Minute  // max wait for agent to come back
	RestartHealthPollInterval     = 10 * time.Second // how often to check health during recovery
	MaxRestartRecoveries          = 1                // max restart recoveries per import/restore
)

// AgentClient is an HTTP client for the Manticore agent
type AgentClient struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
}

// NewAgentClient creates a new agent client with authentication
func NewAgentClient(baseURL, authToken string) *AgentClient {
	return &AgentClient{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Minute, // Long timeout for imports
		},
	}
}

// doRequest performs an HTTP request with bearer token authentication and optional retries
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) doRequest(method, path string, body interface{}, maxRetries int) ([]byte, error) {
	const defaultMaxRetries = 5
	const retryDelay = 3 * time.Second

	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	var jsonBody []byte
	var err error
	if body != nil {
		jsonBody, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal body: %w", err)
		}
	}

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		var bodyReader io.Reader
		if jsonBody != nil {
			bodyReader = bytes.NewReader(jsonBody)
		}

		req, err := http.NewRequest(method, c.baseURL+path, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+c.authToken)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			slog.Debug("request failed", "host", c.baseURL, "path", path, "attempt", attempt, "maxRetries", maxRetries, "error", err)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
			continue
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read response: %w", err)
			slog.Debug("failed to read response", "host", c.baseURL, "path", path, "attempt", attempt, "maxRetries", maxRetries, "error", err)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
			continue
		}

		// Retry on 5xx errors (server errors, including 503 Service Unavailable)
		if resp.StatusCode >= 500 {
			var errResp types.Response
			json.Unmarshal(respBody, &errResp)
			lastErr = fmt.Errorf("API error (%d): %s", resp.StatusCode, errResp.Error)
			slog.Debug("server error", "host", c.baseURL, "path", path, "attempt", attempt, "maxRetries", maxRetries, "statusCode", resp.StatusCode, "error", errResp.Error)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
			continue
		}

		// Non-retryable client errors (4xx)
		if resp.StatusCode >= 400 {
			var errResp types.Response
			json.Unmarshal(respBody, &errResp)
			return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, errResp.Error)
		}

		return respBody, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// Health returns the agent health status
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) Health(maxRetries int) (*types.HealthResponse, error) {
	body, err := c.doRequest("GET", "/api/health", nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp types.HealthResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &resp, nil
}

// QueryCount returns the number of queries processed by the Manticore instance on this replica
func (c *AgentClient) QueryCount(maxRetries int) (int64, error) {
	body, err := c.doRequest("GET", "/api/metrics/query-count", nil, maxRetries)
	if err != nil {
		return 0, err
	}

	var resp struct {
		QueryCount int64 `json:"queryCount"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.QueryCount, nil
}

// HealthProbe does a single health check without retries to quickly determine if a replica exists.
// Returns error if the replica returns 503 (pod doesn't exist) or is unreachable.
func (c *AgentClient) HealthProbe() (*types.HealthResponse, error) {
	req, err := http.NewRequest("GET", c.baseURL+"/api/health", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.authToken)

	// Short timeout for probe
	probeClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := probeClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("probe failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// 503 means the pod doesn't exist (CPLN infra response)
	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, fmt.Errorf("replica unavailable (503)")
	}

	// Any other error status
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("probe returned status %d", resp.StatusCode)
	}

	var healthResp types.HealthResponse
	if err := json.Unmarshal(body, &healthResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &healthResp, nil
}

// WaitForHealth polls the agent health endpoint until it responds successfully or the timeout expires.
// Used to wait for an agent to recover after a pod restart.
func (c *AgentClient) WaitForHealth(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(RestartHealthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("agent did not recover within %v", timeout)
			}
			if _, err := c.HealthProbe(); err != nil {
				slog.Debug("agent not yet healthy", "error", err)
				continue
			}
			slog.Info("agent health recovered", "baseURL", c.baseURL)
			return nil
		}
	}
}

// Grastate returns the grastate.dat information for cluster repair decisions
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) Grastate(maxRetries int) (*types.GrastateResponse, error) {
	body, err := c.doRequest("GET", "/api/grastate", nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp types.GrastateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &resp, nil
}

// ListTables returns all tables
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) ListTables(maxRetries int) ([]types.TableInfo, error) {
	body, err := c.doRequest("GET", "/api/tables", nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp types.TablesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.Tables, nil
}

// CreateTable creates an RT table
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) CreateTable(table string, maxRetries int) error {
	_, err := c.doRequest("POST", "/api/table/create", map[string]string{"table": table}, maxRetries)
	return err
}

// DropTable drops a table
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) DropTable(table string, maxRetries int) error {
	_, err := c.doRequest("POST", "/api/table/drop", map[string]string{"table": table}, maxRetries)
	return err
}

// AlterDistributed alters a distributed table's local references and agents
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) AlterDistributed(distributed string, locals []string, agents []string, haStrategy string, agentRetryCount int, maxRetries int) error {
	req := types.AlterDistributedRequest{
		Distributed:     distributed,
		Locals:          locals,
		Agents:          agents,
		HAStrategy:      haStrategy,
		AgentRetryCount: agentRetryCount,
	}
	_, err := c.doRequest("POST", "/api/distributed/alter", req, maxRetries)
	return err
}

// CreateDistributed creates a distributed table with optional agents for load balancing
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) CreateDistributed(distributed string, locals []string, agents []string, haStrategy string, agentRetryCount int, maxRetries int) error {
	req := types.CreateDistributedRequest{
		Distributed:     distributed,
		Locals:          locals,
		Agents:          agents,
		HAStrategy:      haStrategy,
		AgentRetryCount: agentRetryCount,
	}
	_, err := c.doRequest("POST", "/api/distributed/create", req, maxRetries)
	return err
}

// ClusterAdd adds a table to the cluster
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) ClusterAdd(table string, maxRetries int) error {
	_, err := c.doRequest("POST", "/api/cluster/add", map[string]string{"table": table}, maxRetries)
	return err
}

// ClusterDrop removes a table from the cluster (required before DROP TABLE)
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) ClusterDrop(table string, maxRetries int) error {
	_, err := c.doRequest("POST", "/api/cluster/drop", map[string]string{"table": table}, maxRetries)
	return err
}

// ClusterBootstrap creates a new cluster on this replica
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) ClusterBootstrap(maxRetries int) error {
	_, err := c.doRequest("POST", "/api/cluster/bootstrap", nil, maxRetries)
	return err
}

// ClusterJoin joins an existing cluster
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) ClusterJoin(sourceAddr string, maxRetries int) error {
	_, err := c.doRequest("POST", "/api/cluster/join", map[string]string{
		"sourceAddr": sourceAddr,
	}, maxRetries)
	return err
}

// ClusterRejoin forces this replica to leave current cluster and join the source
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) ClusterRejoin(sourceAddr string, maxRetries int) error {
	_, err := c.doRequest("POST", "/api/cluster/rejoin", map[string]string{
		"sourceAddr": sourceAddr,
	}, maxRetries)
	return err
}

// ImportConfig holds configuration for async import polling
type ImportConfig struct {
	PollInterval      time.Duration
	PollTimeout       time.Duration
	Resume            bool
	Method            types.ImportMethod // Import method (bulk or indexer)
	MemLimit          string             // Memory limit for indexer (e.g., "2G", "4G")
	PrebuiltIndexPath string             // Path to pre-built index on S3 mount (for indexer method)
}

// DefaultImportConfig returns default import configuration
func DefaultImportConfig() ImportConfig {
	return ImportConfig{
		PollInterval: 5 * time.Second,
		PollTimeout:  30 * time.Minute,
	}
}

// ImportConfigFromEnv creates an ImportConfig from environment variables
func ImportConfigFromEnv() ImportConfig {
	config := DefaultImportConfig()

	if interval := os.Getenv("IMPORT_POLL_INTERVAL"); interval != "" {
		if d, err := time.ParseDuration(interval); err == nil {
			config.PollInterval = d
		}
	}

	if timeout := os.Getenv("IMPORT_POLL_TIMEOUT"); timeout != "" {
		if d, err := time.ParseDuration(timeout); err == nil {
			config.PollTimeout = d
		}
	}

	if resume := os.Getenv("IMPORT_RESUME"); resume != "" {
		if r, err := strconv.ParseBool(resume); err == nil {
			config.Resume = r
		}
	}

	if method := os.Getenv("IMPORT_METHOD"); method != "" {
		config.Method = types.ImportMethod(method)
	}

	if memLimit := os.Getenv("IMPORT_MEM_LIMIT"); memLimit != "" {
		config.MemLimit = memLimit
	}

	return config
}

// StartImport initiates an async import and returns the job ID
func (c *AgentClient) StartImport(req types.ImportRequest, maxRetries int) (string, error) {
	body, err := c.doRequest("POST", "/api/import", req, maxRetries)
	if err != nil {
		return "", err
	}

	var resp types.StartImportResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.JobID, nil
}

// GetImportStatus returns the status of an import job
func (c *AgentClient) GetImportStatus(jobID string, maxRetries int) (*types.ImportJob, error) {
	body, err := c.doRequest("GET", fmt.Sprintf("/api/import/%s", jobID), nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp types.ImportJobResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.Job, nil
}

// CancelImport cancels a running import job
func (c *AgentClient) CancelImport(jobID string, maxRetries int) error {
	_, err := c.doRequest("DELETE", fmt.Sprintf("/api/import/%s", jobID), nil, maxRetries)
	return err
}

// Import performs an async import with polling (backward compatible)
// This replaces the old synchronous Import method
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) Import(table, csvPath, cluster string, maxRetries int) error {
	return c.ImportWithContext(context.Background(), table, csvPath, cluster, maxRetries, ImportConfigFromEnv())
}

// ImportWithConfig performs an async import with custom polling configuration
func (c *AgentClient) ImportWithConfig(table, csvPath, cluster string, maxRetries int, config ImportConfig) error {
	return c.ImportWithContext(context.Background(), table, csvPath, cluster, maxRetries, config)
}

// ImportWithContext performs an async import with context support for cancellation.
// Includes restart resilience: if the agent becomes unreachable (e.g. pod restart),
// waits up to 5 minutes for recovery and re-submits the import.
func (c *AgentClient) ImportWithContext(ctx context.Context, table, csvPath, cluster string, maxRetries int, config ImportConfig) error {
	// Build import request from config
	req := types.ImportRequest{
		Table:             table,
		CSVPath:           csvPath,
		Cluster:           cluster,
		Resume:            config.Resume,
		Method:            config.Method,
		MemLimit:          config.MemLimit,
		PrebuiltIndexPath: config.PrebuiltIndexPath,
	}

	// Start the async import
	jobID, err := c.StartImport(req, maxRetries)
	if err != nil {
		return fmt.Errorf("failed to start import: %w", err)
	}

	slog.Debug("import job started", "jobId", jobID, "table", table, "method", config.Method)

	// Poll for completion with restart recovery
	deadline := time.Now().Add(config.PollTimeout)
	ticker := time.NewTicker(config.PollInterval)
	defer ticker.Stop()

	consecutiveFailures := 0
	restartRecoveries := 0

	for {
		select {
		case <-ctx.Done():
			slog.Info("import cancelled by context", "jobId", jobID)
			c.CancelImport(jobID, 1)
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				c.CancelImport(jobID, 1)
				return fmt.Errorf("import timeout after %v", config.PollTimeout)
			}

			job, err := c.GetImportStatus(jobID, 1) // Use 1 retry for fast failure detection
			if err != nil {
				consecutiveFailures++
				slog.Warn("failed to get import status",
					"jobId", jobID, "error", err,
					"consecutiveFailures", consecutiveFailures)

				if consecutiveFailures >= RestartMaxConsecutiveFailures {
					if restartRecoveries >= MaxRestartRecoveries {
						return fmt.Errorf("agent unreachable after %d restart recovery attempt(s): %w", restartRecoveries, err)
					}

					slog.Warn("agent appears to have restarted, waiting for recovery",
						"table", table, "baseURL", c.baseURL)

					if err := c.WaitForHealth(ctx, RestartRecoveryTimeout); err != nil {
						return fmt.Errorf("agent did not recover: %w", err)
					}

					// Agent is back — re-submit the import
					slog.Info("re-submitting import after agent restart", "table", table)
					newJobID, err := c.StartImport(req, maxRetries)
					if err != nil {
						return fmt.Errorf("failed to re-start import after restart: %w", err)
					}

					jobID = newJobID
					consecutiveFailures = 0
					restartRecoveries++
					// Extend deadline since we lost time waiting for recovery
					deadline = time.Now().Add(config.PollTimeout)

					slog.Info("import re-submitted after restart",
						"newJobId", jobID, "table", table,
						"recoveryAttempt", restartRecoveries)
				}
				continue
			}

			// Successful poll — reset failure counter
			consecutiveFailures = 0

			switch job.Status {
			case types.ImportJobStatusCompleted:
				slog.Debug("import job completed", "jobId", jobID)
				return nil
			case types.ImportJobStatusFailed:
				return fmt.Errorf("import failed: %s", job.Error)
			case types.ImportJobStatusCancelled:
				return fmt.Errorf("import was cancelled")
			case types.ImportJobStatusPending, types.ImportJobStatusRunning:
				slog.Debug("import job still running", "jobId", jobID, "status", job.Status)
			}
		}
	}
}

// BaseURL returns the client's base URL
func (c *AgentClient) BaseURL() string {
	return c.baseURL
}

// GetTableSchema returns the schema of a table
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) GetTableSchema(tableName string, maxRetries int) (*schema.TableSchemaResponse, error) {
	body, err := c.doRequest("GET", fmt.Sprintf("/api/tables/%s/schema", tableName), nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp schema.TableSchemaResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &resp, nil
}

// TableConfigColumn represents a column definition from the schema registry
type TableConfigColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// TableConfigResponse represents table behavior configuration from the agent
type TableConfigResponse struct {
	Table           string              `json:"table"`
	ImportMethod    string              `json:"importMethod"`
	ClusterMain     bool                `json:"clusterMain"`
	HAStrategy      string              `json:"haStrategy"`
	AgentRetryCount int                 `json:"agentRetryCount"`
	MemLimit        string              `json:"memLimit,omitempty"`
	HasHeader       *bool               `json:"hasHeader,omitempty"`
	Columns         []TableConfigColumn `json:"columns,omitempty"`
}

// GetTableConfig returns the behavior configuration for a table
// maxRetries: 1 = no retries, 0 = use default (5)
func (c *AgentClient) GetTableConfig(tableName string, maxRetries int) (*TableConfigResponse, error) {
	body, err := c.doRequest("GET", fmt.Sprintf("/api/tables/%s/config", tableName), nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp TableConfigResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &resp, nil
}

// StartBackup initiates an async backup and returns the job ID
func (c *AgentClient) StartBackup(req types.BackupRequest, maxRetries int) (string, error) {
	body, err := c.doRequest("POST", "/api/backup", req, maxRetries)
	if err != nil {
		return "", err
	}

	var resp types.StartBackupResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.JobID, nil
}

// GetBackupStatus returns the status of a backup job
func (c *AgentClient) GetBackupStatus(jobID string, maxRetries int) (*types.BackupJob, error) {
	body, err := c.doRequest("GET", fmt.Sprintf("/api/backup/%s", jobID), nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp types.BackupJobResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.Job, nil
}

// StartRestore initiates an async restore and returns the job ID
func (c *AgentClient) StartRestore(req types.RestoreRequest, maxRetries int) (string, error) {
	body, err := c.doRequest("POST", "/api/restore", req, maxRetries)
	if err != nil {
		return "", err
	}

	var resp types.StartBackupResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.JobID, nil
}

// GetRestoreStatus returns the status of a restore job
func (c *AgentClient) GetRestoreStatus(jobID string, maxRetries int) (*types.BackupJob, error) {
	body, err := c.doRequest("GET", fmt.Sprintf("/api/restore/%s", jobID), nil, maxRetries)
	if err != nil {
		return nil, err
	}

	var resp types.BackupJobResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.Job, nil
}
