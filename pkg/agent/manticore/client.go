package manticore

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/shared/schema"
	_ "github.com/go-sql-driver/mysql"
)

// Client wraps a MySQL connection and HTTP client to Manticore
type Client struct {
	db         *sql.DB
	httpClient *http.Client
	host       string
	port       string // MySQL port (9306)
	httpPort   string // HTTP API port (9308)
}

// NewClient creates a new Manticore client with MySQL and HTTP support
func NewClient(host, port, httpPort string) (*Client, error) {
	dsn := fmt.Sprintf("tcp(%s:%s)/", host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	return &Client{
		db:         db,
		httpClient: &http.Client{Timeout: 60 * time.Second},
		host:       host,
		port:       port,
		httpPort:   httpPort,
	}, nil
}

// Close closes the database connection
func (c *Client) Close() error {
	return c.db.Close()
}

// Host returns the MySQL host
func (c *Client) Host() string {
	return c.host
}

// Port returns the MySQL port
func (c *Client) Port() string {
	return c.port
}

// HTTPPort returns the HTTP API port
func (c *Client) HTTPPort() string {
	return c.httpPort
}

// BulkInsert sends NDJSON to Manticore's bulk HTTP API endpoint.
// The ndjson parameter should be newline-delimited JSON with one operation per line.
// Example: {"insert":{"index":"table","id":1,"doc":{"field":"value"}}}
func (c *Client) BulkInsert(ndjson string) error {
	url := fmt.Sprintf("http://%s:%s/bulk", c.host, c.httpPort)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(ndjson))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bulk insert failed: %s - %s", resp.Status, string(body))
	}

	return nil
}

// Execute runs a SQL statement
func (c *Client) Execute(sql string) error {
	_, err := c.db.Exec(sql)
	return err
}

// Query runs a SQL query and returns rows
func (c *Client) Query(sql string) (*sql.Rows, error) {
	return c.db.Query(sql)
}

// GetStatus returns basic searchd status
func (c *Client) GetStatus() (map[string]string, error) {
	rows, err := c.db.Query("SHOW STATUS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	status := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			continue
		}
		status[key] = value
	}
	return status, nil
}

// GetClusterStatus returns cluster status
func (c *Client) GetClusterStatus(clusterName string) (string, error) {
	query := fmt.Sprintf("SHOW STATUS LIKE 'cluster_%s_status'", clusterName)
	rows, err := c.db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return "", err
		}
		return value, nil
	}
	return "unknown", nil
}

// GetClusterNodeState returns the cluster node state (e.g., "synced", "donor", etc.)
func (c *Client) GetClusterNodeState(clusterName string) (string, error) {
	query := fmt.Sprintf("SHOW STATUS LIKE 'cluster_%s_node_state'", clusterName)
	rows, err := c.db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return "", err
		}
		return value, nil
	}
	return "unknown", nil
}

// GetClusterHealth returns both cluster status and node state
func (c *Client) GetClusterHealth(clusterName string) (status string, nodeState string, err error) {
	status, err = c.GetClusterStatus(clusterName)
	if err != nil {
		return "", "", err
	}
	nodeState, err = c.GetClusterNodeState(clusterName)
	if err != nil {
		return status, "", err
	}
	return status, nodeState, nil
}

// TableInfo represents information about a table
type TableInfo struct {
	Name string
	Type string
}

// ListTables returns all tables
func (c *Client) ListTables() ([]TableInfo, error) {
	rows, err := c.db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var name, tableType string
		if err := rows.Scan(&name, &tableType); err != nil {
			continue
		}
		tables = append(tables, TableInfo{Name: name, Type: tableType})
	}
	return tables, nil
}

// TableExists checks if a table exists
func (c *Client) TableExists(name string) (bool, error) {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", name)
	rows, err := c.db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}

// GetDistributedLocals returns the local tables referenced by a distributed table
// Returns nil if table doesn't exist or isn't distributed
func (c *Client) GetDistributedLocals(tableName string) ([]string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil // Table doesn't exist
	}

	var tbl, createStmt string
	if err := rows.Scan(&tbl, &createStmt); err != nil {
		return nil, err
	}
	return parseLocalsFromTableCreateStatement(createStmt)
}

func parseLocalsFromTableCreateStatement(createStmt string) ([]string, error) {
	// Parse local='...' from create statement
	// Example: CREATE TABLE products type='distributed' local='products_main_b' local='products_delta'
	var locals []string
	for _, part := range strings.Split(createStmt, " ") {
		if strings.HasPrefix(part, "local='") {
			local := strings.TrimPrefix(part, "local='")
			local = strings.TrimSuffix(local, "'")
			locals = append(locals, local)
		}
	}
	return locals, nil
}

// GetClusterTables returns a set of table names that are in the cluster
func (c *Client) GetClusterTables(clusterName string) (map[string]bool, error) {
	query := fmt.Sprintf("SHOW STATUS LIKE 'cluster_%s_indexes'", clusterName)
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]bool)
	if rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		// value is comma-separated list of table names
		if value != "" {
			for _, table := range strings.Split(value, ",") {
				trimmed := strings.TrimSpace(table)
				if trimmed != "" {
					result[trimmed] = true
				}
			}
		}
	}
	return result, nil
}

// DescribeTable returns the schema of a table using DESCRIBE command
func (c *Client) DescribeTable(tableName string) ([]schema.ColumnSchema, error) {
	query := fmt.Sprintf("DESCRIBE %s", tableName)
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []schema.ColumnSchema{}
	for rows.Next() {
		var field, colType string
		var props *string
		// DESCRIBE returns: Field, Type, Properties (Properties may be NULL)
		if err := rows.Scan(&field, &colType, &props); err != nil {
			// Try scanning without properties column (some versions may not have it)
			if err := rows.Scan(&field, &colType); err != nil {
				continue
			}
		}
		col := schema.ColumnSchema{
			Field: field,
			Type:  colType,
		}
		if props != nil {
			col.Props = *props
		}
		columns = append(columns, col)
	}
	return columns, nil
}
