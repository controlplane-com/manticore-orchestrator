package indexer

import (
	"fmt"
	"strings"
)

// GenerateIndexerConfig creates the indexer.conf content for building a plain index
func GenerateIndexerConfig(cfg *Config) string {
	var sb strings.Builder

	// Determine delimiter based on file extension
	delimiter := ","
	if strings.HasSuffix(strings.ToLower(cfg.SourcePath), ".tsv") {
		delimiter = "\\t"
	}

	// Build awk command to add line numbers as IDs
	// NR>1 skips header if present, otherwise NR>=1
	startLine := "1"
	if cfg.HasHeader {
		startLine = "2"
	}
	awkCmd := fmt.Sprintf("awk -F'%s' 'BEGIN{OFS=\"\\t\"} NR>=%s {$1=$1; print NR-%s \"\\t\" $0}' %s",
		delimiter, startLine, startLine, cfg.SourcePath)

	// Source definition
	sb.WriteString(fmt.Sprintf("source %s_source {\n", cfg.PlainName))
	sb.WriteString("    type = tsvpipe\n")
	sb.WriteString(fmt.Sprintf("    tsvpipe_command = %s\n", awkCmd))
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
	sb.WriteString("}\n\n")

	// Plain index (for ATTACH source)
	sb.WriteString(fmt.Sprintf("index %s {\n", cfg.PlainName))
	sb.WriteString("    type = plain\n")
	sb.WriteString(fmt.Sprintf("    path = %s/data/%s\n", cfg.WorkDir, cfg.PlainName))
	sb.WriteString("}\n\n")

	// RT index (target for ATTACH)
	sb.WriteString(fmt.Sprintf("index %s {\n", cfg.TableName))
	sb.WriteString("    type = rt\n")
	sb.WriteString(fmt.Sprintf("    path = %s/data/%s\n", cfg.WorkDir, cfg.TableName))

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
