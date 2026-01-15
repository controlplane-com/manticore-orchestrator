package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/agent/manticore"
	"github.com/controlplane-com/manticore-orchestrator/pkg/import/config"
	"github.com/controlplane-com/manticore-orchestrator/pkg/import/generator"
	"github.com/controlplane-com/manticore-orchestrator/pkg/import/parser"
)

// convertColumns converts manticore.Column slice to config.Column slice
func convertColumns(cols []manticore.Column) []config.Column {
	result := make([]config.Column, len(cols))
	for i, col := range cols {
		result[i] = config.Column{
			Name: col.Name,
			Type: config.ColumnType(col.Type),
		}
	}
	return result
}

// ImportOptions configures the worker pool for a CSV import
type ImportOptions struct {
	Table        string
	Columns      []config.Column
	BatchSize    int
	WorkerCount  int
	MySQLHost    string // Host for Manticore (used for both MySQL and HTTP)
	HTTPPort     string // HTTP API port for bulk endpoint
	ErrorLogPath string
	SkipHeader   bool
}

// csvRecord represents a parsed CSV row with its line number
type csvRecord struct {
	record  []string
	lineNum int
}

// ImportWorkerPool manages parallel CSV import with multiple worker goroutines
type ImportWorkerPool struct {
	table       string
	columns     []config.Column
	batchSize   int
	workerCount int
	mysqlHost   string
	httpPort    string
	skipHeader  bool

	// Channel for distributing records to workers
	records chan csvRecord

	// Progress tracking (atomic for concurrent access)
	processedLines int64
	failedLines    int64
	lastLineNum    int64

	// Error logging
	errorLogPath string
	errorLogMu   sync.Mutex

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Worker synchronization
	wg sync.WaitGroup

	// Producer error (set if producer fails)
	producerErr error
	producerMu  sync.Mutex
}

// NewImportWorkerPool creates a new worker pool for CSV import
func NewImportWorkerPool(ctx context.Context, opts ImportOptions) *ImportWorkerPool {
	poolCtx, cancel := context.WithCancel(ctx)

	return &ImportWorkerPool{
		table:        opts.Table,
		columns:      opts.Columns,
		batchSize:    opts.BatchSize,
		workerCount:  opts.WorkerCount,
		mysqlHost:    opts.MySQLHost,
		httpPort:     opts.HTTPPort,
		skipHeader:   opts.SkipHeader,
		errorLogPath: opts.ErrorLogPath,
		records:      make(chan csvRecord, opts.WorkerCount*opts.BatchSize),
		ctx:          poolCtx,
		cancel:       cancel,
	}
}

// Run starts the import process and blocks until completion or cancellation
func (p *ImportWorkerPool) Run(csvPath string, startLine int64) error {
	slog.Info("starting import worker pool",
		"table", p.table,
		"workers", p.workerCount,
		"batchSize", p.batchSize,
		"csvPath", csvPath,
		"startLine", startLine)

	// Start workers
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	// Start producer
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		if err := p.producer(csvPath, startLine); err != nil {
			p.producerMu.Lock()
			p.producerErr = err
			p.producerMu.Unlock()
			// Cancel workers on producer error
			p.cancel()
		}
	}()

	// Wait for producer to finish
	<-producerDone

	// Close records channel to signal workers to finish
	close(p.records)

	// Wait for all workers to complete
	p.wg.Wait()

	// Check for producer error
	p.producerMu.Lock()
	err := p.producerErr
	p.producerMu.Unlock()

	if err != nil && err != context.Canceled {
		return err
	}

	// Check if cancelled
	if p.ctx.Err() == context.Canceled {
		return context.Canceled
	}

	return nil
}

// Cancel stops the import process gracefully
func (p *ImportWorkerPool) Cancel() {
	p.cancel()
}

// Progress returns current progress counters
func (p *ImportWorkerPool) Progress() (processed, failed, lastLine int64) {
	return atomic.LoadInt64(&p.processedLines),
		atomic.LoadInt64(&p.failedLines),
		atomic.LoadInt64(&p.lastLineNum)
}

// producer reads the CSV file and sends records to the channel
func (p *ImportWorkerPool) producer(csvPath string, startLine int64) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer f.Close()

	// Detect delimiter from file extension
	delimiter := ','
	if strings.HasSuffix(strings.ToLower(csvPath), ".tsv") {
		delimiter = '\t'
	}

	proc, err := parser.NewCSVProcessor(f, p.skipHeader, delimiter)
	if err != nil {
		return fmt.Errorf("failed to create CSV processor: %w", err)
	}

	// Read and dispatch records
	for {
		// Check for cancellation
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}

		record, lineNum, err := proc.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			p.recordError(lineNum, err)
			continue
		}

		// Skip lines until we reach the resume point
		if int64(lineNum) <= startLine {
			continue
		}

		// Send record to workers (blocking if channel is full)
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case p.records <- csvRecord{record: record, lineNum: lineNum}:
		}
	}

	return nil
}

// worker processes records from the channel and sends them to Manticore's bulk API
func (p *ImportWorkerPool) worker(id int) {
	defer p.wg.Done()

	// Create HTTP client for this worker
	httpClient := &http.Client{Timeout: 60 * time.Second}

	// Create Bulk generator for this worker
	gen := generator.NewBulkGenerator(p.table, p.columns, p.batchSize)

	for {
		select {
		case <-p.ctx.Done():
			// Flush remaining batch before exit
			if ndjson := gen.Flush(); ndjson != "" {
				if err := p.executeBulk(httpClient, ndjson); err != nil {
					slog.Warn("worker failed to flush batch on cancel", "workerId", id, "error", err)
				}
			}
			return

		case rec, ok := <-p.records:
			if !ok {
				// Channel closed, flush remaining batch and exit
				if ndjson := gen.Flush(); ndjson != "" {
					if err := p.executeBulk(httpClient, ndjson); err != nil {
						slog.Warn("worker failed to flush final batch", "workerId", id, "error", err)
						p.recordError(0, fmt.Errorf("flush batch: %w", err))
					}
				}
				return
			}

			// Convert row to JSON values
			docID, values, err := parser.ConvertRowJSON(rec.record, p.columns, rec.lineNum)
			if err != nil {
				p.recordError(rec.lineNum, err)
				atomic.AddInt64(&p.failedLines, 1)
				continue
			}

			// Add to batch
			if ndjson := gen.AddRow(docID, values); ndjson != "" {
				if err := p.executeBulk(httpClient, ndjson); err != nil {
					// Log error but don't count individual rows as failed
					// since some may have succeeded in the batch
					slog.Warn("worker bulk insert failed", "workerId", id, "error", err)
					p.recordError(rec.lineNum, fmt.Errorf("bulk insert failed: %w", err))
				}
			}

			atomic.AddInt64(&p.processedLines, 1)
			// Update last line number (checkpoint)
			for {
				old := atomic.LoadInt64(&p.lastLineNum)
				if int64(rec.lineNum) <= old {
					break
				}
				if atomic.CompareAndSwapInt64(&p.lastLineNum, old, int64(rec.lineNum)) {
					break
				}
			}
		}
	}
}

// executeBulk sends NDJSON to Manticore's bulk API
func (p *ImportWorkerPool) executeBulk(client *http.Client, ndjson string) error {
	url := fmt.Sprintf("http://%s:%s/bulk", p.mysqlHost, p.httpPort)

	req, err := http.NewRequestWithContext(p.ctx, "POST", url, bytes.NewBufferString(ndjson))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := client.Do(req)
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

// recordError logs an error to the error log file
func (p *ImportWorkerPool) recordError(lineNum int, err error) {
	p.errorLogMu.Lock()
	defer p.errorLogMu.Unlock()

	f, openErr := os.OpenFile(p.errorLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if openErr != nil {
		slog.Error("failed to open error log", "path", p.errorLogPath, "error", openErr)
		return
	}
	defer f.Close()

	if lineNum > 0 {
		fmt.Fprintf(f, "line %d: %v\n", lineNum, err)
	} else {
		fmt.Fprintf(f, "error: %v\n", err)
	}
}
