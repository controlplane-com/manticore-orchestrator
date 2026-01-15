package handlers

import (
	"context"
	"testing"

	"github.com/controlplane-com/manticore-orchestrator/pkg/api/agent/manticore"
	"github.com/controlplane-com/manticore-orchestrator/pkg/import/config"
)

func TestConvertColumns(t *testing.T) {
	manticoreCols := []manticore.Column{
		{Name: "title", Type: "field"},
		{Name: "price", Type: "attr_float"},
		{Name: "count", Type: "attr_uint"},
	}

	configCols := convertColumns(manticoreCols)

	if len(configCols) != 3 {
		t.Fatalf("len(configCols) = %d, want 3", len(configCols))
	}

	tests := []struct {
		idx     int
		name    string
		typeStr config.ColumnType
	}{
		{0, "title", "field"},
		{1, "price", "attr_float"},
		{2, "count", "attr_uint"},
	}

	for _, tc := range tests {
		if configCols[tc.idx].Name != tc.name {
			t.Errorf("configCols[%d].Name = %q, want %q", tc.idx, configCols[tc.idx].Name, tc.name)
		}
		if configCols[tc.idx].Type != tc.typeStr {
			t.Errorf("configCols[%d].Type = %q, want %q", tc.idx, configCols[tc.idx].Type, tc.typeStr)
		}
	}
}

func TestNewImportWorkerPool(t *testing.T) {
	ctx := context.Background()
	opts := ImportOptions{
		Table:        "test_table",
		Columns:      []config.Column{{Name: "title", Type: "field"}},
		BatchSize:    100,
		WorkerCount:  4,
		MySQLHost:    "localhost",
		HTTPPort:     "9306",
		ErrorLogPath: "/tmp/test.error.log",
		SkipHeader:   true,
	}

	pool := NewImportWorkerPool(ctx, opts)

	if pool == nil {
		t.Fatal("NewImportWorkerPool() returned nil")
	}
	if pool.table != "test_table" {
		t.Errorf("table = %q, want 'test_table'", pool.table)
	}
	if pool.batchSize != 100 {
		t.Errorf("batchSize = %d, want 100", pool.batchSize)
	}
	if pool.workerCount != 4 {
		t.Errorf("workerCount = %d, want 4", pool.workerCount)
	}
	if pool.skipHeader != true {
		t.Errorf("skipHeader = %v, want true", pool.skipHeader)
	}
}

func TestImportWorkerPoolProgress(t *testing.T) {
	ctx := context.Background()
	opts := ImportOptions{
		Table:        "test_table",
		Columns:      []config.Column{{Name: "title", Type: "field"}},
		BatchSize:    100,
		WorkerCount:  4,
		MySQLHost:    "localhost",
		HTTPPort:     "9306",
		ErrorLogPath: "/tmp/test.error.log",
		SkipHeader:   true,
	}

	pool := NewImportWorkerPool(ctx, opts)

	// Initial progress should be zero
	processed, failed, lastLine := pool.Progress()
	if processed != 0 {
		t.Errorf("initial processed = %d, want 0", processed)
	}
	if failed != 0 {
		t.Errorf("initial failed = %d, want 0", failed)
	}
	if lastLine != 0 {
		t.Errorf("initial lastLine = %d, want 0", lastLine)
	}
}

func TestImportWorkerPoolCancel(t *testing.T) {
	ctx := context.Background()
	opts := ImportOptions{
		Table:        "test_table",
		Columns:      []config.Column{{Name: "title", Type: "field"}},
		BatchSize:    100,
		WorkerCount:  4,
		MySQLHost:    "localhost",
		HTTPPort:     "9306",
		ErrorLogPath: "/tmp/test.error.log",
		SkipHeader:   true,
	}

	pool := NewImportWorkerPool(ctx, opts)

	// Cancel should not panic
	pool.Cancel()

	// After cancel, context should be done
	select {
	case <-pool.ctx.Done():
		// Expected
	default:
		t.Error("context was not cancelled")
	}
}
