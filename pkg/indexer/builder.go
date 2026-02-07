package indexer

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// IndexBuilder builds Manticore indexes from CSV/TSV source files
type IndexBuilder struct {
	workDir string
}

// NewIndexBuilder creates a new IndexBuilder with the specified work directory
func NewIndexBuilder(workDir string) *IndexBuilder {
	return &IndexBuilder{workDir: workDir}
}

// Build executes the full indexer-based build process and returns the path to the RT index
// This does NOT import the table into Manticore - it only builds the index files
func (b *IndexBuilder) Build(ctx context.Context, cfg *Config) (*BuildResult, error) {
	// Override work dir if provided in config
	workDir := b.workDir
	if cfg.WorkDir != "" {
		workDir = cfg.WorkDir
	}

	// Set defaults
	if cfg.MemLimit == "" {
		cfg.MemLimit = DefaultMemLimit
	}
	if cfg.ImportPort == 0 {
		cfg.ImportPort = DefaultImportPort
	}
	if cfg.ImportMySQL == 0 {
		cfg.ImportMySQL = DefaultImportMySQLPort
	}
	if cfg.PlainName == "" {
		cfg.PlainName = cfg.TableName + "_plain"
	}

	// Check if source file has header if not already set
	if !cfg.HasHeader {
		hasHeader, err := csvHasHeader(cfg.SourcePath)
		if err != nil {
			return nil, fmt.Errorf("failed to check CSV header: %w", err)
		}
		cfg.HasHeader = hasHeader
	}

	cfg.WorkDir = workDir

	// Log column info for debugging
	var colNames []string
	for _, col := range cfg.Columns {
		colNames = append(colNames, fmt.Sprintf("%s(%s)", col.Name, col.Type))
	}
	slog.Info("starting indexer build",
		"table", cfg.TableName,
		"source", cfg.SourcePath,
		"memLimit", cfg.MemLimit,
		"hasHeader", cfg.HasHeader,
		"columnCount", len(cfg.Columns),
		"columns", strings.Join(colNames, ", "),
		"workDir", workDir)

	// Step 1: Create work directories
	// (must happen before preprocessing so we can write the TSV file)
	dirs := []string{
		filepath.Join(workDir, "data"),
		filepath.Join(workDir, "log"),
		filepath.Join(workDir, "binlog"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Step 2: Preprocess source file into TSV with IDs for the indexer
	// This avoids relying on awk/sed being available in the container
	tsvPath := filepath.Join(workDir, "source.tsv")
	if err := preprocessToTSV(cfg.SourcePath, tsvPath, cfg.HasHeader); err != nil {
		return nil, fmt.Errorf("failed to preprocess source file: %w", err)
	}
	cfg.SourcePath = tsvPath
	slog.Debug("preprocessed source to TSV", "path", tsvPath)

	// Debug: log first 3 lines of preprocessed TSV
	if preview, err := previewFile(tsvPath, 3); err == nil {
		slog.Debug("preprocessed TSV preview", "lines", preview)
	}

	// Step 3: Generate and write indexer.conf
	indexerConfPath := filepath.Join(workDir, "indexer.conf")
	indexerConf := GenerateIndexerConfig(cfg)
	if err := os.WriteFile(indexerConfPath, []byte(indexerConf), 0644); err != nil {
		return nil, fmt.Errorf("failed to write indexer.conf: %w", err)
	}
	slog.Debug("wrote indexer.conf", "path", indexerConfPath, "content", indexerConf)

	// Step 4: Generate and write searchd.conf
	searchdConfPath := filepath.Join(workDir, "searchd.conf")
	searchdConf := GenerateSearchdConfig(cfg)
	if err := os.WriteFile(searchdConfPath, []byte(searchdConf), 0644); err != nil {
		return nil, fmt.Errorf("failed to write searchd.conf: %w", err)
	}
	slog.Debug("wrote searchd.conf", "path", searchdConfPath)

	// Cleanup function to stop searchd if it was started
	var searchdStarted bool
	cleanup := func() {
		if searchdStarted {
			slog.Debug("stopping temporary searchd")
			stopCmd := exec.Command("searchd", "--config", searchdConfPath, "--stop")
			stopCmd.Run() // Ignore errors during cleanup
		}
	}
	defer cleanup()

	// Step 5: Run indexer to build plain index
	slog.Info("running indexer to build plain index", "index", cfg.PlainName)
	indexerCmd := exec.CommandContext(ctx, "indexer", "--config", indexerConfPath, cfg.PlainName)
	indexerOutput, err := indexerCmd.CombinedOutput()
	if err != nil {
		slog.Error("indexer failed", "error", err, "output", string(indexerOutput))
		return nil, fmt.Errorf("indexer failed: %w\nOutput: %s", err, string(indexerOutput))
	}
	slog.Debug("indexer completed", "output", string(indexerOutput))

	// Step 6: Start temporary searchd
	slog.Info("starting temporary searchd", "port", cfg.ImportMySQL)
	searchdCmd := exec.CommandContext(ctx, "searchd", "--config", searchdConfPath)
	searchdOutput, err := searchdCmd.CombinedOutput()
	if err != nil {
		slog.Error("searchd start failed", "error", err, "output", string(searchdOutput))
		return nil, fmt.Errorf("searchd start failed: %w\nOutput: %s", err, string(searchdOutput))
	}
	searchdStarted = true
	slog.Debug("searchd started", "output", string(searchdOutput))

	// Step 7: Wait for searchd to be ready
	if err := waitForSearchd(ctx, cfg.ImportMySQL, 30*time.Second); err != nil {
		return nil, fmt.Errorf("searchd not ready: %w", err)
	}
	slog.Debug("searchd is ready")

	// Step 8: Connect to temporary searchd and run ATTACH INDEX
	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/", cfg.ImportMySQL)
	tempDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to temporary searchd: %w", err)
	}
	defer tempDB.Close()

	attachSQL := fmt.Sprintf("ATTACH INDEX %s TO RTINDEX %s", cfg.PlainName, cfg.TableName)
	slog.Info("attaching plain index to RT", "sql", attachSQL)
	if _, err := tempDB.ExecContext(ctx, attachSQL); err != nil {
		return nil, fmt.Errorf("ATTACH INDEX failed: %w", err)
	}

	// Step 9: Verify row count
	var rowCount int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", cfg.TableName)
	if err := tempDB.QueryRowContext(ctx, countSQL).Scan(&rowCount); err != nil {
		slog.Warn("failed to get row count", "error", err)
	} else {
		slog.Info("RT table row count", "table", cfg.TableName, "count", rowCount)
	}

	// Step 10: Stop temporary searchd
	slog.Info("stopping temporary searchd")
	tempDB.Close() // Close connection first
	stopCmd := exec.Command("searchd", "--config", searchdConfPath, "--stop")
	if output, err := stopCmd.CombinedOutput(); err != nil {
		slog.Warn("searchd stop returned error", "error", err, "output", string(output))
	}
	searchdStarted = false

	// Wait a moment for searchd to fully stop and release files
	time.Sleep(1 * time.Second)

	// Return the path to the RT index folder
	rtDataPath := filepath.Join(workDir, "data", cfg.TableName)

	slog.Info("indexer build completed successfully",
		"table", cfg.TableName,
		"rows", rowCount,
		"indexPath", rtDataPath)

	return &BuildResult{
		IndexPath: rtDataPath,
		RowCount:  rowCount,
	}, nil
}

// waitForSearchd polls until the temporary searchd is accepting connections
func waitForSearchd(ctx context.Context, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	dsn := fmt.Sprintf("tcp(127.0.0.1:%d)/", port)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		db, err := sql.Open("mysql", dsn)
		if err == nil {
			err = db.Ping()
			db.Close()
			if err == nil {
				return nil
			}
		}

		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for searchd on port %d", port)
}

// previewFile reads the first n lines of a file for debug logging
func previewFile(path string, n int) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var lines []string
	for i := 0; i < n && scanner.Scan(); i++ {
		lines = append(lines, scanner.Text())
	}
	return strings.Join(lines, "\n"), nil
}

// preprocessToTSV reads a CSV or TSV source file and writes a TSV with auto-generated
// document IDs as the first column. This avoids relying on awk in the container.
func preprocessToTSV(srcPath, dstPath string, hasHeader bool) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create TSV: %w", err)
	}
	defer dst.Close()

	writer := bufio.NewWriter(dst)
	defer writer.Flush()

	// Detect delimiter from file extension
	delimiter := ","
	if strings.HasSuffix(strings.ToLower(srcPath), ".tsv") {
		delimiter = "\t"
	}

	scanner := bufio.NewScanner(src)
	// Increase buffer size for lines that may be very long
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNum := 0
	docID := 0
	for scanner.Scan() {
		lineNum++
		// Skip header if present
		if hasHeader && lineNum == 1 {
			continue
		}

		line := scanner.Text()
		if line == "" {
			continue
		}

		// Split on source delimiter, rejoin with tabs
		fields := strings.Split(line, delimiter)
		fmt.Fprintf(writer, "%d\t%s\n", docID, strings.Join(fields, "\t"))
		docID++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading source file: %w", err)
	}

	return nil
}

// csvHasHeader checks if the first field of the first row is not a number (indicating a header)
func csvHasHeader(csvPath string) (bool, error) {
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
