package cpln

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is a Control Plane API client
type Client struct {
	token   string
	org     string
	baseURL string
	client  *http.Client
}

// AutoscalingConfig represents workload autoscaling configuration
type AutoscalingConfig struct {
	MaxScale int    `json:"maxScale"`
	MinScale int    `json:"minScale"`
	Metric   string `json:"metric"`
}

// DefaultOptions represents workload default options
type DefaultOptions struct {
	Autoscaling AutoscalingConfig `json:"autoscaling"`
}

// WorkloadSpec represents the workload specification
type WorkloadSpec struct {
	Type           string         `json:"type"`
	DefaultOptions DefaultOptions `json:"defaultOptions"`
}

// Workload represents a Control Plane workload
type Workload struct {
	Name string       `json:"name"`
	Spec WorkloadSpec `json:"spec"`
}

// NewClient creates a new Control Plane API client
func NewClient(token, org string) *Client {
	return &Client{
		token:   token,
		org:     org,
		baseURL: "http://api.cpln.io",
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// NewClientWithTimeout creates a new Control Plane API client with custom timeout
func NewClientWithTimeout(token, org string, timeout time.Duration) *Client {
	return &Client{
		token:   token,
		org:     org,
		baseURL: "http://api.cpln.io",
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Token returns the client's token (for use by extended clients)
func (c *Client) Token() string {
	return c.token
}

// Org returns the client's organization
func (c *Client) Org() string {
	return c.org
}

// BaseURL returns the client's base URL
func (c *Client) BaseURL() string {
	return c.baseURL
}

// HTTPClient returns the underlying HTTP client
func (c *Client) HTTPClient() *http.Client {
	return c.client
}

// DoRequest performs an authenticated HTTP request to the CPLN API
func (c *Client) DoRequest(method, path string, body interface{}) ([]byte, error) {
	url := c.baseURL + path

	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// DoRequestWithHeaders performs an authenticated HTTP request and returns body + headers
func (c *Client) DoRequestWithHeaders(method, path string, body interface{}) ([]byte, http.Header, error) {
	url := c.baseURL + path

	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, nil, fmt.Errorf("API error (%d): %s", resp.StatusCode, string(respBody))
	}

	return respBody, resp.Header, nil
}

// GetWorkload fetches a workload definition from Control Plane
func (c *Client) GetWorkload(gvc, name string) (*Workload, error) {
	path := fmt.Sprintf("/org/%s/gvc/%s/workload/%s", c.org, gvc, name)

	body, err := c.DoRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var workload Workload
	if err := json.Unmarshal(body, &workload); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &workload, nil
}

// GetReplicaCount returns the maxScale for a workload (maximum number of replicas)
func (c *Client) GetReplicaCount(gvc, workloadName string) (int, error) {
	workload, err := c.GetWorkload(gvc, workloadName)
	if err != nil {
		return 0, err
	}

	maxScale := workload.Spec.DefaultOptions.Autoscaling.MaxScale
	if maxScale <= 0 {
		return 1, nil // Default to 1 if not set
	}

	return maxScale, nil
}

// Command represents a CPLN workload command
type Command struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	LifecycleStage string                 `json:"lifecycleStage"`
	Spec           map[string]interface{} `json:"spec"`
	Status         map[string]interface{} `json:"status,omitempty"`
	Created        string                 `json:"created,omitempty"` // ISO timestamp from CPLN API
}

// CommandList represents a list of commands
type CommandList struct {
	Kind  string    `json:"kind"`
	Items []Command `json:"items"`
	Links []Link    `json:"links,omitempty"`
}

// Link represents a CPLN API link for pagination
type Link struct {
	Rel  string `json:"rel"`
	Href string `json:"href"`
}

// findNextLink returns the href of the "next" link, or empty string if not found
func findNextLink(links []Link) string {
	for _, link := range links {
		if link.Rel == "next" {
			return link.Href
		}
	}
	return ""
}

// fetchPages follows pagination links and accumulates items up to the specified limit.
// If limit is 0, fetches all pages. Returns when limit is reached or no more pages exist.
func (c *Client) fetchPages(initialList *CommandList, limit int) error {
	nextURL := findNextLink(initialList.Links)

	for nextURL != "" {
		// Stop if we've reached the limit
		if limit > 0 && len(initialList.Items) >= limit {
			break
		}

		// The next link is relative, DoRequest will prepend baseURL
		respBody, err := c.DoRequest("GET", nextURL, nil)
		if err != nil {
			return fmt.Errorf("failed to fetch next page: %w", err)
		}

		var nextPage CommandList
		if err := json.Unmarshal(respBody, &nextPage); err != nil {
			return fmt.Errorf("failed to decode next page: %w", err)
		}

		// Append items from this page
		initialList.Items = append(initialList.Items, nextPage.Items...)

		// Update links for potential further iteration
		initialList.Links = nextPage.Links

		// Get next page URL
		nextURL = findNextLink(nextPage.Links)
	}

	// Trim to limit if exceeded
	if limit > 0 && len(initialList.Items) > limit {
		initialList.Items = initialList.Items[:limit]
	}

	return nil
}

// ContainerOverride for cron workload commands
type ContainerOverride struct {
	Name string   `json:"name"`
	Env  []EnvVar `json:"env,omitempty"`
}

// EnvVar represents an environment variable
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// RunCronWorkloadSpec for starting cron workloads
type RunCronWorkloadSpec struct {
	Location           string              `json:"location"`
	ContainerOverrides []ContainerOverride `json:"containerOverrides,omitempty"`
}

// QueryTerm represents a query term for filtering
type QueryTerm struct {
	Op       string `json:"op"`
	Property string `json:"property"`
	Value    string `json:"value"`
}

// SortSpec represents sort configuration for queries
type SortSpec struct {
	By    string `json:"by"`
	Order string `json:"order"`
}

// QuerySpec represents a query specification
type QuerySpec struct {
	Sort  *SortSpec   `json:"sort,omitempty"`
	Match string      `json:"match"`
	Terms []QueryTerm `json:"terms"`
}

// QueryRequest represents a query request body
type QueryRequest struct {
	Spec QuerySpec `json:"spec"`
}

// CreateCommand creates a command on a workload
// The CPLN API returns "Created" as the body with the command ID in the Location header
func (c *Client) CreateCommand(gvc, workload string, cmdType string, spec interface{}) (*Command, error) {
	path := fmt.Sprintf("/org/%s/gvc/%s/workload/%s/-command", c.org, gvc, workload)
	body := map[string]interface{}{
		"type": cmdType,
		"spec": spec,
	}
	_, headers, err := c.DoRequestWithHeaders("POST", path, body)
	if err != nil {
		return nil, err
	}

	// Extract command ID from Location header
	// Format: /org/{org}/gvc/{gvc}/workload/{workload}/-command/{commandId}
	location := headers.Get("Location")
	parts := strings.Split(location, "/-command/")
	if len(parts) != 2 || parts[1] == "" {
		return nil, fmt.Errorf("invalid or missing Location header: %q", location)
	}
	commandID := parts[1]

	return &Command{ID: commandID, Type: cmdType}, nil
}

// QueryCommands queries commands on a workload with filters.
// If limit is 0, fetches all pages; otherwise stops when limit items are collected.
func (c *Client) QueryCommands(gvc, workload string, terms []QueryTerm, limit int) (*CommandList, error) {
	path := fmt.Sprintf("/org/%s/gvc/%s/workload/%s/-command/-query", c.org, gvc, workload)
	body := QueryRequest{
		Spec: QuerySpec{
			Match: "all",
			Terms: terms,
		},
	}
	respBody, err := c.DoRequest("POST", path, body)
	if err != nil {
		return nil, err
	}
	var list CommandList
	if err := json.Unmarshal(respBody, &list); err != nil {
		return nil, fmt.Errorf("failed to decode command list: %w", err)
	}

	// Follow pagination up to limit
	if err := c.fetchPages(&list, limit); err != nil {
		return nil, err
	}

	return &list, nil
}

// QueryCommandsByLifecycleStage queries commands by lifecycle stage (pending, running, completed, failed).
// If limit is 0, fetches all pages; otherwise stops when limit items are collected.
func (c *Client) QueryCommandsByLifecycleStage(gvc, workload, lifecycleStage string, limit int) (*CommandList, error) {
	return c.QueryCommands(gvc, workload, []QueryTerm{
		{Op: "=", Property: "lifecycleStage", Value: lifecycleStage},
	}, limit)
}

// QueryActiveCommands queries for commands that are pending or running.
// If limit is 0, fetches all pages; otherwise stops when limit items are collected.
func (c *Client) QueryActiveCommands(gvc, workload string, limit int) (*CommandList, error) {
	path := fmt.Sprintf("/org/%s/gvc/%s/workload/%s/-command/-query", c.org, gvc, workload)
	body := QueryRequest{
		Spec: QuerySpec{
			Match: "any", // Match ANY of the terms (OR)
			Terms: []QueryTerm{
				{Op: "=", Property: "lifecycleStage", Value: "pending"},
				{Op: "=", Property: "lifecycleStage", Value: "running"},
			},
		},
	}
	respBody, err := c.DoRequest("POST", path, body)
	if err != nil {
		return nil, err
	}
	var list CommandList
	if err := json.Unmarshal(respBody, &list); err != nil {
		return nil, fmt.Errorf("failed to decode command list: %w", err)
	}

	// Follow pagination up to limit
	if err := c.fetchPages(&list, limit); err != nil {
		return nil, err
	}

	return &list, nil
}

// QueryAllCommands queries all runCronWorkload commands (all lifecycle stages) for command history.
// If limit is 0, fetches all pages; otherwise stops when limit items are collected.
func (c *Client) QueryAllCommands(gvc, workload string, limit int) (*CommandList, error) {
	path := fmt.Sprintf("/org/%s/gvc/%s/workload/%s/-command/-query", c.org, gvc, workload)
	body := QueryRequest{
		Spec: QuerySpec{
			Sort: &SortSpec{
				By:    "created",
				Order: "desc",
			},
			Match: "all",
			Terms: []QueryTerm{
				{Op: "=", Property: "type", Value: "runCronWorkload"},
			},
		},
	}
	respBody, err := c.DoRequest("POST", path, body)
	if err != nil {
		return nil, err
	}
	var list CommandList
	if err := json.Unmarshal(respBody, &list); err != nil {
		return nil, fmt.Errorf("failed to decode command list: %w", err)
	}

	// Follow pagination up to limit
	if err := c.fetchPages(&list, limit); err != nil {
		return nil, err
	}

	return &list, nil
}

// StartCronWorkload triggers a cron workload run
func (c *Client) StartCronWorkload(gvc, workload, location string, containerOverrides []ContainerOverride) (*Command, error) {
	spec := RunCronWorkloadSpec{
		Location:           location,
		ContainerOverrides: containerOverrides,
	}
	return c.CreateCommand(gvc, workload, "runCronWorkload", spec)
}

// DeploymentVersion represents a single replica's deployment status
type DeploymentVersion struct {
	Name       string                     `json:"name"`
	Ready      bool                       `json:"ready"`
	Message    string                     `json:"message"`
	Containers map[string]ContainerStatus `json:"containers"`
}

// ContainerStatus represents a container's status within a deployment
type ContainerStatus struct {
	Name    string `json:"name"`
	Ready   bool   `json:"ready"`
	Message string `json:"message"`
}

// DeploymentStatus represents the status of a deployment
type DeploymentStatus struct {
	Ready    bool                `json:"ready"`
	Message  string              `json:"message"`
	Versions []DeploymentVersion `json:"versions"`
}

// Deployment represents a Control Plane deployment (per-location)
type Deployment struct {
	Name   string           `json:"name"`
	Status DeploymentStatus `json:"status"`
}

// DeploymentList represents a list of deployments
type DeploymentList struct {
	Items []Deployment `json:"items"`
}

// GetDeployments fetches all deployments for a workload
func (c *Client) GetDeployments(gvc, workload string) (*DeploymentList, error) {
	path := fmt.Sprintf("/org/%s/gvc/%s/workload/%s/deployment", c.org, gvc, workload)
	body, err := c.DoRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var list DeploymentList
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("failed to parse deployments: %w", err)
	}
	return &list, nil
}
