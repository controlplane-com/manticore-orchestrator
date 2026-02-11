package manticore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// ColumnType represents a column type mapping
type ColumnType int

const (
	ColumnTypeField ColumnType = iota
	ColumnTypeFieldString
	ColumnTypeUint
	ColumnTypeBigint
	ColumnTypeFloat
	ColumnTypeBool
	ColumnTypeString
	ColumnTypeTimestamp
	ColumnTypeMulti
	ColumnTypeMulti64
	ColumnTypeJSON
)

// Column represents a column definition
type Column struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"`
}

// Schema represents the parsed schema configuration
type Schema struct {
	Columns         []Column
	JSONConfig      string // Raw JSON for passing to csv-to-manticore
	ImportMethod    string // "bulk" or "indexer", defaults to "bulk"
	ClusterMain     bool   // Whether to add main table to cluster, defaults to true
	HAStrategy      string // HA strategy for distributed table mirrors, defaults to "nodeads"
	AgentRetryCount int    // Retry count for failed agents, defaults to 0
	MemLimit        string // Indexer memory limit, defaults to "2G"
	HasHeader       *bool  // Whether source file has a header row, nil = auto-detect
}

// csv-to-manticore type to ColumnType mapping
var typeStringMap = map[string]ColumnType{
	"field":          ColumnTypeField,
	"field_string":   ColumnTypeFieldString,
	"attr_uint":      ColumnTypeUint,
	"attr_bigint":    ColumnTypeBigint,
	"attr_float":     ColumnTypeFloat,
	"attr_bool":      ColumnTypeBool,
	"attr_string":    ColumnTypeString,
	"attr_timestamp": ColumnTypeTimestamp,
	"attr_multi":     ColumnTypeMulti,
	"attr_multi_64":  ColumnTypeMulti64,
	"attr_json":      ColumnTypeJSON,
}

// RT table type names for CREATE TABLE
var rtTypeNames = map[ColumnType]string{
	ColumnTypeField:       "text",
	ColumnTypeFieldString: "text",
	ColumnTypeUint:        "uint",
	ColumnTypeBigint:      "bigint",
	ColumnTypeFloat:       "float",
	ColumnTypeBool:        "bool",
	ColumnTypeString:      "string",
	ColumnTypeTimestamp:   "timestamp",
	ColumnTypeMulti:       "multi",
	ColumnTypeMulti64:     "multi64",
	ColumnTypeJSON:        "json",
}

// GetColumnType returns the ColumnType for a type string
func GetColumnType(typeStr string) ColumnType {
	if ct, ok := typeStringMap[typeStr]; ok {
		return ct
	}
	return ColumnTypeField // default to field
}

// GetRTTypeName returns the RT table type name for a ColumnType
func GetRTTypeName(ct ColumnType) string {
	if name, ok := rtTypeNames[ct]; ok {
		return name
	}
	return "text" // default
}

// GenerateCreateTableSQL generates a CREATE TABLE statement for an RT table
func (s *Schema) GenerateCreateTableSQL(tableName string) string {
	var columns []string

	// First column is always id
	columns = append(columns, "id bigint")

	// Add schema columns
	for _, col := range s.Columns {
		ct := GetColumnType(col.Type)
		rtType := GetRTTypeName(ct)
		columns = append(columns, fmt.Sprintf("%s %s", col.Name, rtType))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, strings.Join(columns, ", "))
}

// CSVHasHeader checks if the first field of the first row is not a number (indicating a header)
func CSVHasHeader(csvPath string) (bool, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return false, fmt.Errorf("failed to open CSV: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return false, fmt.Errorf("CSV file is empty")
	}

	firstLine := scanner.Text()
	if firstLine == "" {
		return false, nil
	}

	// Get the first field (before first comma)
	firstField := strings.Split(firstLine, ",")[0]
	firstField = strings.TrimSpace(firstField)

	// Check if first field is a valid number (integer)
	_, err = strconv.ParseInt(firstField, 10, 64)
	if err != nil {
		// Not a number, so it's a header
		return true, nil
	}

	return false, nil
}

// SchemaRegistry holds multiple schemas keyed by table name
type SchemaRegistry struct {
	schemas map[string]*Schema
}

// TableBehaviorConfig holds per-table behavior settings
type TableBehaviorConfig struct {
	ImportMethod    string `yaml:"importMethod" json:"importMethod"`       // "bulk" or "indexer"
	ClusterMain     *bool  `yaml:"clusterMain" json:"clusterMain"`         // Use pointer for nil-check (default true)
	HAStrategy      string `yaml:"haStrategy" json:"haStrategy"`           // "random", "roundrobin", "nodeads" (default), "noerrors"
	AgentRetryCount *int   `yaml:"agentRetryCount" json:"agentRetryCount"` // Pointer for nil-check (default 0)
	MemLimit        string `yaml:"memLimit" json:"memLimit"`               // Indexer memory limit (default "2G")
	HasHeader       *bool  `yaml:"hasHeader" json:"hasHeader"`             // Whether source file has a header row (default: auto-detect)
}

// SchemaConfig represents the YAML structure for a single table schema (JSON format)
type SchemaConfig struct {
	Schema struct {
		Columns []Column `yaml:"columns"`
	} `yaml:"schema"`
	Config TableBehaviorConfig `yaml:"config"`
}

// NewSchemaRegistry creates a new empty schema registry
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{schemas: make(map[string]*Schema)}
}

// LoadFromFile parses a YAML file containing multiple table schemas (JSON column format)
func (r *SchemaRegistry) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	var configs map[string]SchemaConfig
	if err := yaml.Unmarshal(data, &configs); err != nil {
		return fmt.Errorf("failed to parse schema YAML: %w", err)
	}

	for tableName, config := range configs {
		// Generate JSON config for csv-to-manticore
		jsonConfig, err := json.Marshal(map[string]interface{}{
			"columns": config.Schema.Columns,
		})
		if err != nil {
			return fmt.Errorf("failed to generate JSON config for %s: %w", tableName, err)
		}

		// Apply defaults for behavior config
		importMethod := "bulk"
		if config.Config.ImportMethod != "" {
			importMethod = config.Config.ImportMethod
		}

		clusterMain := true
		if config.Config.ClusterMain != nil {
			clusterMain = *config.Config.ClusterMain
		}

		haStrategy := "nodeads"
		if config.Config.HAStrategy != "" {
			haStrategy = config.Config.HAStrategy
		}

		agentRetryCount := 0
		if config.Config.AgentRetryCount != nil {
			agentRetryCount = *config.Config.AgentRetryCount
		}

		r.schemas[tableName] = &Schema{
			Columns:         config.Schema.Columns,
			JSONConfig:      string(jsonConfig),
			ImportMethod:    importMethod,
			ClusterMain:     clusterMain,
			HAStrategy:      haStrategy,
			AgentRetryCount: agentRetryCount,
			MemLimit:        config.Config.MemLimit,
			HasHeader:       config.Config.HasHeader,
		}
	}

	return nil
}

// Get retrieves a schema by table name
func (r *SchemaRegistry) Get(tableName string) (*Schema, bool) {
	schema, ok := r.schemas[tableName]
	return schema, ok
}

// GetForDerivedTable extracts base table name and retrieves the schema
// Handles names like addresses_main_a, addresses_main_b, addresses_delta
func (r *SchemaRegistry) GetForDerivedTable(tableName string) (*Schema, bool) {
	baseName := ExtractBaseTableName(tableName)
	return r.Get(baseName)
}

// List returns a sorted list of table names in the registry
func (r *SchemaRegistry) List() []string {
	names := make([]string, 0, len(r.schemas))
	for name := range r.schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ExtractBaseTableName extracts the base table name from derived names
// e.g., addresses_main_a -> addresses, products_delta -> products
func ExtractBaseTableName(tableName string) string {
	suffixes := []string{"_main_a", "_main_b", "_delta"}
	for _, suffix := range suffixes {
		if strings.HasSuffix(tableName, suffix) {
			return strings.TrimSuffix(tableName, suffix)
		}
	}
	return tableName
}
