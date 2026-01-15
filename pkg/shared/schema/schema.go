package schema

// ColumnSchema represents a single column in a table schema
type ColumnSchema struct {
	Field string `json:"field"`
	Type  string `json:"type"`
	Props string `json:"props,omitempty"`
}

// TableSchemaResponse represents the response for table schema queries
type TableSchemaResponse struct {
	Table   string         `json:"table"`
	Columns []ColumnSchema `json:"columns"`
}
