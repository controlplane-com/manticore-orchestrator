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

	slog.Info("starting indexer build",
		"table", cfg.TableName,
		"source", cfg.SourcePath,
		"memLimit", cfg.MemLimit,
		"hasHeader", cfg.HasHeader,
		"workDir", workDir)

	// Step 1: Create work directories
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

	// Step 2: Generate and write indexer.conf
	indexerConfPath := filepath.Join(workDir, "indexer.conf")
	indexerConf := GenerateIndexerConfig(cfg)
	if err := os.WriteFile(indexerConfPath, []byte(indexerConf), 0644); err != nil {
		return nil, fmt.Errorf("failed to write indexer.conf: %w", err)
	}
	slog.Debug("wrote indexer.conf", "path", indexerConfPath)

	// Step 3: Generate and write searchd.conf
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

	// Step 4: Run indexer to build plain index
	slog.Info("running indexer to build plain index", "index", cfg.PlainName)
	indexerCmd := exec.CommandContext(ctx, "indexer", "--config", indexerConfPath, cfg.PlainName)
	indexerOutput, err := indexerCmd.CombinedOutput()
	if err != nil {
		slog.Error("indexer failed", "error", err, "output", string(indexerOutput))
		return nil, fmt.Errorf("indexer failed: %w\nOutput: %s", err, string(indexerOutput))
	}
	slog.Debug("indexer completed", "output", string(indexerOutput))

	// Step 5: Start temporary searchd
	slog.Info("starting temporary searchd", "port", cfg.ImportMySQL)
	searchdCmd := exec.CommandContext(ctx, "searchd", "--config", searchdConfPath)
	searchdOutput, err := searchdCmd.CombinedOutput()
	if err != nil {
		slog.Error("searchd start failed", "error", err, "output", string(searchdOutput))
		return nil, fmt.Errorf("searchd start failed: %w\nOutput: %s", err, string(searchdOutput))
	}
	searchdStarted = true
	slog.Debug("searchd started", "output", string(searchdOutput))

	// Step 6: Wait for searchd to be ready
	if err := waitForSearchd(ctx, cfg.ImportMySQL, 30*time.Second); err != nil {
		return nil, fmt.Errorf("searchd not ready: %w", err)
	}
	slog.Debug("searchd is ready")

	// Step 7: Connect to temporary searchd and run ATTACH INDEX
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

	// Step 8: Verify row count
	var rowCount int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", cfg.TableName)
	if err := tempDB.QueryRowContext(ctx, countSQL).Scan(&rowCount); err != nil {
		slog.Warn("failed to get row count", "error", err)
	} else {
		slog.Info("RT table row count", "table", cfg.TableName, "count", rowCount)
	}

	// Step 9: Stop temporary searchd
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
