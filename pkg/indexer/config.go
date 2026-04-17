package indexer

import (
	"fmt"
	"strings"
)

// GenerateIndexerConfig creates the indexer.conf content for building a plain index
// cfg.SourcePath should point to a preprocessed TSV file (with ID as first column)
func GenerateIndexerConfig(cfg *Config) string {
	var sb strings.Builder

	// Source definition - always use tsvpipe with cat (source is preprocessed TSV)
	sb.WriteString(fmt.Sprintf("source %s_source {\n", cfg.PlainName))
	sb.WriteString("    type = tsvpipe\n")
	sb.WriteString(fmt.Sprintf("    tsvpipe_command = cat %s\n", cfg.SourcePath))
	sb.WriteString("\n")

	// Column definitions
	for _, col := range cfg.Columns {
		directive, ok := TsvpipeDirectives[col.Type]
		if !ok {
			directive = "tsvpipe_field" // default to field
		}
		sb.WriteString(fmt.Sprintf("    %s = %s\n", directive, col.Name))
	}
	sb.WriteString("}\n\n")

	// Plain index definition
	// Note: charset_table is intentionally omitted — SQL keyword aliases like "non_cont"
	// are not valid in the sphinx-style indexer config format. The production RT table's
	// charset is set via CREATE TABLE SQL, not here.
	sb.WriteString(fmt.Sprintf("index %s {\n", cfg.PlainName))
	sb.WriteString("    type = plain\n")
	sb.WriteString(fmt.Sprintf("    source = %s_source\n", cfg.PlainName))
	sb.WriteString(fmt.Sprintf("    path = %s/data/%s\n", cfg.WorkDir, cfg.PlainName))
	sb.WriteString("}\n\n")

	// Indexer settings
	sb.WriteString("indexer {\n")
	sb.WriteString(fmt.Sprintf("    mem_limit = %s\n", cfg.MemLimit))
	sb.WriteString("}\n")

	return sb.String()
}

// GenerateSearchdConfig creates the searchd.conf for the temporary instance
func GenerateSearchdConfig(cfg *Config) string {
	var sb strings.Builder

	// Searchd daemon settings
	sb.WriteString("searchd {\n")
	sb.WriteString(fmt.Sprintf("    listen = %d\n", cfg.ImportPort))
	sb.WriteString(fmt.Sprintf("    listen = %d:mysql\n", cfg.ImportMySQL))
	sb.WriteString(fmt.Sprintf("    log = %s/log/searchd.log\n", cfg.WorkDir))
	sb.WriteString(fmt.Sprintf("    query_log = %s/log/query.log\n", cfg.WorkDir))
	sb.WriteString(fmt.Sprintf("    pid_file = %s/searchd.pid\n", cfg.WorkDir))
	sb.WriteString(fmt.Sprintf("    binlog_path = %s/binlog\n", cfg.WorkDir))
	// Load the columnar library so REBUILD SECONDARY works during the build phase,
	// writing the .spidx file to the shared volume for IMPORT TABLE to pick up on all replicas.
	sb.WriteString("    plugin_dir = /usr/share/manticore/modules\n")
	sb.WriteString("}\n\n")

	// Plain index (for ATTACH source) — no charset_table, must match RT below
	sb.WriteString(fmt.Sprintf("index %s {\n", cfg.PlainName))
	sb.WriteString("    type = plain\n")
	sb.WriteString(fmt.Sprintf("    path = %s/data/%s\n", cfg.WorkDir, cfg.PlainName))
	sb.WriteString("}\n\n")

	// RT index (target for ATTACH) — no charset_table so it matches the plain index above,
	// allowing ATTACH to succeed. The production RT's charset is set via CREATE TABLE SQL.
	// secondary_indexes=1 enables the PGM secondary index so REBUILD SECONDARY can write
	// the .spidx file into the work directory before the index is imported to all replicas.
	sb.WriteString(fmt.Sprintf("index %s {\n", cfg.TableName))
	sb.WriteString("    type = rt\n")
	sb.WriteString(fmt.Sprintf("    path = %s/data/%s\n", cfg.WorkDir, cfg.TableName))
	sb.WriteString("    secondary_indexes=1\n")

	// RT column definitions
	for _, col := range cfg.Columns {
		directive, ok := RtAttrDirectives[col.Type]
		if !ok {
			directive = "rt_field" // default to field
		}
		sb.WriteString(fmt.Sprintf("    %s = %s\n", directive, col.Name))
	}
	sb.WriteString("}\n")

	return sb.String()
}
