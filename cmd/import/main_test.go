package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/cuppojoe/csv-to-manticore/pkg/config"
)

// failingWriter simulates a broken pipe by returning an error after N writes
type failingWriter struct {
	writesBeforeFail int
	writeCount       int
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	w.writeCount++
	if w.writeCount > w.writesBeforeFail {
		return 0, errors.New("broken pipe")
	}
	return len(p), nil
}

func TestProcessCSV_Success(t *testing.T) {
	input := "1,Widget,19.99\n2,Gadget,29.99\n"
	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "name", Type: config.TypeAttrString},
			{Name: "price", Type: config.TypeAttrFloat},
		},
	}

	var output bytes.Buffer
	err := processCSV(strings.NewReader(input), cfg, "products", false, 1, ',', &output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := output.String()
	if !strings.Contains(result, "INSERT INTO products") {
		t.Errorf("expected INSERT statement, got: %s", result)
	}
	if !strings.Contains(result, "'Widget'") {
		t.Errorf("expected 'Widget' in output, got: %s", result)
	}
}

func TestProcessCSV_BrokenPipe(t *testing.T) {
	// Create input with multiple rows
	input := "1,Widget,19.99\n2,Gadget,29.99\n3,Thing,39.99\n"
	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "name", Type: config.TypeAttrString},
			{Name: "price", Type: config.TypeAttrFloat},
		},
	}

	// Writer that fails after first write (simulating mysql dying)
	writer := &failingWriter{writesBeforeFail: 1}

	err := processCSV(strings.NewReader(input), cfg, "products", false, 1, ',', writer)
	if err == nil {
		t.Fatal("expected error due to broken pipe, got nil")
	}
	if !strings.Contains(err.Error(), "write error") {
		t.Errorf("expected 'write error' in message, got: %v", err)
	}
}

func TestProcessCSV_BrokenPipeOnFlush(t *testing.T) {
	// With batch size 10 and only 3 rows, the flush at the end will write
	input := "1,Widget,19.99\n2,Gadget,29.99\n3,Thing,39.99\n"
	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "name", Type: config.TypeAttrString},
			{Name: "price", Type: config.TypeAttrFloat},
		},
	}

	// Writer that fails on first write (the flush)
	writer := &failingWriter{writesBeforeFail: 0}

	err := processCSV(strings.NewReader(input), cfg, "products", false, 10, ',', writer)
	if err == nil {
		t.Fatal("expected error due to broken pipe on flush, got nil")
	}
	if !strings.Contains(err.Error(), "write error") {
		t.Errorf("expected 'write error' in message, got: %v", err)
	}
}

func TestProcessCSV_SkipsParseErrors(t *testing.T) {
	// Row 2 has invalid price "bad"
	input := "1,Widget,19.99\n2,Gadget,bad\n3,Thing,39.99\n"
	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "name", Type: config.TypeAttrString},
			{Name: "price", Type: config.TypeAttrFloat},
		},
	}

	var output bytes.Buffer
	err := processCSV(strings.NewReader(input), cfg, "products", false, 1, ',', &output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := output.String()
	// Should have 2 INSERT statements (rows 1 and 3), not 3
	count := strings.Count(result, "INSERT INTO")
	if count != 2 {
		t.Errorf("expected 2 INSERT statements (skipping bad row), got %d: %s", count, result)
	}
}

func TestProcessCSV_TSV(t *testing.T) {
	input := "1\tWidget\t19.99\n2\tGadget\t29.99\n"
	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "name", Type: config.TypeAttrString},
			{Name: "price", Type: config.TypeAttrFloat},
		},
	}

	var output bytes.Buffer
	err := processCSV(strings.NewReader(input), cfg, "products", false, 1, '\t', &output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := output.String()
	if !strings.Contains(result, "'Widget'") {
		t.Errorf("expected 'Widget' in output, got: %s", result)
	}
}
