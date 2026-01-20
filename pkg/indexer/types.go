package indexer

// Column represents a column definition for indexer configuration
type Column struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"`
}

// Config holds paths and settings for indexer-based import
type Config struct {
	WorkDir     string
	TableName   string
	PlainName   string // Plain index name (tableName_plain)
	SourcePath  string
	Columns     []Column
	ImportPort  int
	ImportMySQL int
	MemLimit    string
	HasHeader   bool
}

// BuildResult contains the output from a successful index build
type BuildResult struct {
	IndexPath string // Path to the RT index folder
	RowCount  int64  // Number of rows in the index
}

// tsvpipeDirectives maps schema column types to indexer tsvpipe directives
var TsvpipeDirectives = map[string]string{
	"field":          "tsvpipe_field",
	"field_string":   "tsvpipe_field_string",
	"attr_string":    "tsvpipe_attr_string",
	"attr_uint":      "tsvpipe_attr_uint",
	"attr_bigint":    "tsvpipe_attr_bigint",
	"attr_float":     "tsvpipe_attr_float",
	"attr_bool":      "tsvpipe_attr_uint", // bool as uint
	"attr_timestamp": "tsvpipe_attr_timestamp",
	"attr_multi":     "tsvpipe_attr_multi",
	"attr_multi_64":  "tsvpipe_attr_multi_64",
	"attr_json":      "tsvpipe_attr_json",
}

// rtAttrDirectives maps schema column types to RT index directives
var RtAttrDirectives = map[string]string{
	"field":          "rt_field",
	"field_string":   "rt_field",
	"attr_string":    "rt_attr_string",
	"attr_uint":      "rt_attr_uint",
	"attr_bigint":    "rt_attr_bigint",
	"attr_float":     "rt_attr_float",
	"attr_bool":      "rt_attr_uint",
	"attr_timestamp": "rt_attr_timestamp",
	"attr_multi":     "rt_attr_multi",
	"attr_multi_64":  "rt_attr_multi_64",
	"attr_json":      "rt_attr_json",
}

// Default ports for temporary searchd instance (avoid conflicts with main searchd)
const (
	DefaultImportPort      = 9400
	DefaultImportMySQLPort = 9406
	DefaultMemLimit        = "2G"
)
