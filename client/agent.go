package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
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

// Response represents a generic API response
type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status        string            `json:"status"`
	Searchd       map[string]string `json:"searchd"`
	Cluster       string            `json:"cluster"`
	ClusterStatus string            `json:"clusterStatus"`
}

// TableInfo represents table information
type TableInfo struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

// TablesResponse represents the tables list response
type TablesResponse struct {
	Tables []TableInfo `json:"tables"`
}

// doRequest performs an HTTP request with bearer token authentication
func (c *AgentClient) doRequest(method, path string, body interface{}) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequest(method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add bearer token authentication
	req.Header.Set("Authorization", "Bearer "+c.authToken)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp Response
		json.Unmarshal(respBody, &errResp)
		return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, errResp.Error)
	}

	return respBody, nil
}

// Health returns the agent health status
func (c *AgentClient) Health() (*HealthResponse, error) {
	body, err := c.doRequest("GET", "/api/health", nil)
	if err != nil {
		return nil, err
	}

	var resp HealthResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &resp, nil
}

// ListTables returns all tables
func (c *AgentClient) ListTables() ([]TableInfo, error) {
	body, err := c.doRequest("GET", "/api/tables", nil)
	if err != nil {
		return nil, err
	}

	var resp TablesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return resp.Tables, nil
}

// CreateTable creates an RT table
func (c *AgentClient) CreateTable(table string) error {
	_, err := c.doRequest("POST", "/api/table/create", map[string]string{"table": table})
	return err
}

// DropTable drops a table
func (c *AgentClient) DropTable(table string) error {
	_, err := c.doRequest("POST", "/api/table/drop", map[string]string{"table": table})
	return err
}

// AlterDistributed alters a distributed table's local references
func (c *AgentClient) AlterDistributed(distributed string, locals []string) error {
	_, err := c.doRequest("POST", "/api/distributed/alter", map[string]interface{}{
		"distributed": distributed,
		"locals":      locals,
	})
	return err
}

// CreateDistributed creates a distributed table
func (c *AgentClient) CreateDistributed(distributed string, locals []string) error {
	_, err := c.doRequest("POST", "/api/distributed/create", map[string]interface{}{
		"distributed": distributed,
		"locals":      locals,
	})
	return err
}

// ClusterAdd adds a table to the cluster
func (c *AgentClient) ClusterAdd(table string) error {
	_, err := c.doRequest("POST", "/api/cluster/add", map[string]string{"table": table})
	return err
}

// Import imports a CSV file into a table
func (c *AgentClient) Import(table, csvPath string) error {
	_, err := c.doRequest("POST", "/api/import", map[string]string{
		"table":   table,
		"csvPath": csvPath,
	})
	return err
}

// BaseURL returns the client's base URL
func (c *AgentClient) BaseURL() string {
	return c.baseURL
}
