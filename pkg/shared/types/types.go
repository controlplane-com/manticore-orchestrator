package types

// Response represents a generic API response
type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

// HealthResponse represents the health check response from an agent
type HealthResponse struct {
	Status        string            `json:"status"`
	Searchd       map[string]string `json:"searchd"`
	Cluster       string            `json:"cluster"`
	ClusterStatus string            `json:"clusterStatus"`
	NodeState     string            `json:"nodeState"` // e.g., "synced", "donor", "joiner"
}

// GrastateResponse represents the grastate.dat information for cluster repair decisions
type GrastateResponse struct {
	UUID            string `json:"uuid"`
	Seqno           int64  `json:"seqno"`
	SafeToBootstrap int    `json:"safeToBootstrap"`
	Exists          bool   `json:"exists"`
}

// TableInfo represents table information with cluster membership
type TableInfo struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	InCluster bool   `json:"inCluster"`
	Slot      string `json:"slot,omitempty"` // "a" or "b" for base tables
}

// TablesResponse represents the tables list response
type TablesResponse struct {
	Tables []TableInfo `json:"tables"`
}

// CreateTableRequest is the request body for creating a table
type CreateTableRequest struct {
	Table string `json:"table"`
}

// DropTableRequest is the request body for dropping a table
type DropTableRequest struct {
	Table string `json:"table"`
}

// AlterDistributedRequest is the request body for altering a distributed table
type AlterDistributedRequest struct {
	Distributed     string   `json:"distributed"`
	Locals          []string `json:"locals"`
	Agents          []string `json:"agents,omitempty"`          // Mirror agent addresses (host:port format)
	HAStrategy      string   `json:"haStrategy,omitempty"`      // "random", "roundrobin", "nodeads", "noerrors"
	AgentRetryCount int      `json:"agentRetryCount,omitempty"` // Retry count for failed agents
}

// CreateDistributedRequest is the request body for creating a distributed table
type CreateDistributedRequest struct {
	Distributed     string   `json:"distributed"`
	Locals          []string `json:"locals"`
	Agents          []string `json:"agents,omitempty"`          // Mirror agent addresses (host:port format)
	HAStrategy      string   `json:"haStrategy,omitempty"`      // "random", "roundrobin", "nodeads", "noerrors"
	AgentRetryCount int      `json:"agentRetryCount,omitempty"` // Retry count for failed agents
}

// ClusterAddRequest is the request body for adding a table to the cluster
type ClusterAddRequest struct {
	Table string `json:"table"`
}

// ClusterDropRequest is the request body for removing a table from the cluster
type ClusterDropRequest struct {
	Table string `json:"table"`
}

// ClusterJoinRequest is the request body for joining a cluster
type ClusterJoinRequest struct {
	SourceAddr string `json:"sourceAddr"` // e.g., "manticore-0.manticore:9312"
}

// ClusterRejoinRequest is the request body for rejoining a cluster
type ClusterRejoinRequest struct {
	SourceAddr string `json:"sourceAddr"` // e.g., "manticore-0.manticore:9312"
}

// ImportRequest is the request body for importing data
type ImportRequest struct {
	Table             string `json:"table"`
	CSVPath           string `json:"csvPath"`
	Cluster           string `json:"cluster,omitempty"`           // Cluster name for clustered imports (optional)
	MemLimit          string `json:"memLimit,omitempty"`          // Memory limit for indexer (e.g., "2G", "4G")
	PrebuiltIndexPath string `json:"prebuiltIndexPath,omitempty"` // Path to pre-built index on S3 mount
}

// ImportJobStatus represents the status of an async import job
type ImportJobStatus string

const (
	ImportJobStatusPending   ImportJobStatus = "pending"
	ImportJobStatusRunning   ImportJobStatus = "running"
	ImportJobStatusCompleted ImportJobStatus = "completed"
	ImportJobStatusFailed    ImportJobStatus = "failed"
	ImportJobStatusCancelled ImportJobStatus = "cancelled"
)

// ImportJob represents an async import job
type ImportJob struct {
	ID                string          `json:"id"`
	Table             string          `json:"table"`
	CSVPath           string          `json:"csvPath"`
	Cluster           string          `json:"cluster,omitempty"`           // Cluster name used for this import
	MemLimit          string          `json:"memLimit,omitempty"`          // Memory limit for indexer
	PrebuiltIndexPath string          `json:"prebuiltIndexPath,omitempty"` // Path to pre-built index
	Status            ImportJobStatus `json:"status"`
	Error             string          `json:"error,omitempty"`
	StartedAt         *int64          `json:"startedAt,omitempty"` // Unix timestamp
	EndedAt           *int64          `json:"endedAt,omitempty"`   // Unix timestamp
}

// StartImportResponse is returned when starting an async import
type StartImportResponse struct {
	JobID string `json:"jobId"`
}

// ImportJobResponse is returned when querying job status
type ImportJobResponse struct {
	Job *ImportJob `json:"job"`
}
