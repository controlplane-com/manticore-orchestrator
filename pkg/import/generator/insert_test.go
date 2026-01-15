package generator

import (
	"testing"

	"github.com/controlplane-com/manticore-orchestrator/pkg/import/config"
)

func TestNewInsertGenerator(t *testing.T) {
	// Columns should NOT include id - it's prepended automatically
	columns := []config.Column{
		{Name: "name", Type: config.TypeField},
	}

	gen := NewInsertGenerator("products", columns, 1)

	if gen == nil {
		t.Fatal("expected generator, got nil")
	}

	if gen.table != "products" {
		t.Errorf("expected table 'products', got %q", gen.table)
	}

	// Should have 2 column names: id (auto-added) + name
	if len(gen.columnNames) != 2 {
		t.Errorf("expected 2 column names (id + name), got %d", len(gen.columnNames))
	}

	if gen.columnNames[0] != "id" {
		t.Errorf("expected first column to be 'id', got %q", gen.columnNames[0])
	}

	if gen.columnNames[1] != "name" {
		t.Errorf("expected second column to be 'name', got %q", gen.columnNames[1])
	}

	if gen.batchSize != 1 {
		t.Errorf("expected batchSize 1, got %d", gen.batchSize)
	}
}

func TestNewInsertGenerator_BatchSizeValidation(t *testing.T) {
	// Columns should NOT include id - it's prepended automatically
	columns := []config.Column{{Name: "name", Type: config.TypeField}}

	tests := []struct {
		batchSize int
		want      int
	}{
		{1, 1},
		{10, 10},
		{0, 1},  // Should default to 1
		{-1, 1}, // Should default to 1
	}

	for _, tt := range tests {
		gen := NewInsertGenerator("test", columns, tt.batchSize)
		if gen.batchSize != tt.want {
			t.Errorf("NewInsertGenerator with batchSize %d: got %d, want %d", tt.batchSize, gen.batchSize, tt.want)
		}
	}
}

func TestInsertGenerator_SingleRow(t *testing.T) {
	// Columns should NOT include id - it's prepended automatically
	columns := []config.Column{
		{Name: "name", Type: config.TypeField},
		{Name: "price", Type: config.TypeAttrFloat},
	}

	gen := NewInsertGenerator("products", columns, 1)

	// AddRow now takes (id, values) separately
	id := "1"
	values := []string{"'Widget'", "19.99"}
	stmt := gen.AddRow(id, values)

	expected := "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 19.99);"
	if stmt != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, stmt)
	}
}

func TestInsertGenerator_Batching(t *testing.T) {
	// Columns should NOT include id - it's prepended automatically
	columns := []config.Column{
		{Name: "name", Type: config.TypeField},
	}

	gen := NewInsertGenerator("products", columns, 3)

	// First two rows should not produce output
	stmt := gen.AddRow("1", []string{"'Widget'"})
	if stmt != "" {
		t.Errorf("expected empty string for row 1, got: %s", stmt)
	}

	stmt = gen.AddRow("2", []string{"'Gadget'"})
	if stmt != "" {
		t.Errorf("expected empty string for row 2, got: %s", stmt)
	}

	// Third row should trigger batch
	stmt = gen.AddRow("3", []string{"'Thing'"})
	expected := "INSERT INTO products (id, name) VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Thing');"
	if stmt != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, stmt)
	}
}

func TestInsertGenerator_Flush(t *testing.T) {
	// Columns should NOT include id - it's prepended automatically
	columns := []config.Column{
		{Name: "name", Type: config.TypeField},
	}

	gen := NewInsertGenerator("products", columns, 5)

	// Add 2 rows (less than batch size)
	gen.AddRow("1", []string{"'Widget'"})
	gen.AddRow("2", []string{"'Gadget'"})

	// Flush should return remaining rows
	stmt := gen.Flush()
	expected := "INSERT INTO products (id, name) VALUES (1, 'Widget'), (2, 'Gadget');"
	if stmt != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, stmt)
	}

	// Second flush should return empty
	stmt = gen.Flush()
	if stmt != "" {
		t.Errorf("expected empty string on second flush, got: %s", stmt)
	}
}

func TestInsertGenerator_FlushEmpty(t *testing.T) {
	// Columns should NOT include id - it's prepended automatically
	columns := []config.Column{{Name: "name", Type: config.TypeField}}
	gen := NewInsertGenerator("test", columns, 10)

	stmt := gen.Flush()
	if stmt != "" {
		t.Errorf("expected empty string for flush with no rows, got: %s", stmt)
	}
}

func TestInsertGenerator_MultipleBatches(t *testing.T) {
	// Columns should NOT include id - it's prepended automatically
	columns := []config.Column{
		{Name: "value", Type: config.TypeAttrUint},
	}

	gen := NewInsertGenerator("items", columns, 2)

	// Batch 1
	gen.AddRow("1", []string{"100"})
	stmt := gen.AddRow("2", []string{"200"})
	if stmt != "INSERT INTO items (id, value) VALUES (1, 100), (2, 200);" {
		t.Errorf("batch 1 unexpected: %s", stmt)
	}

	// Batch 2
	gen.AddRow("3", []string{"300"})
	stmt = gen.AddRow("4", []string{"400"})
	if stmt != "INSERT INTO items (id, value) VALUES (3, 300), (4, 400);" {
		t.Errorf("batch 2 unexpected: %s", stmt)
	}

	// Partial batch
	gen.AddRow("5", []string{"500"})
	stmt = gen.Flush()
	if stmt != "INSERT INTO items (id, value) VALUES (5, 500);" {
		t.Errorf("partial batch unexpected: %s", stmt)
	}
}

func TestGenerateSingle(t *testing.T) {
	columnNames := []string{"id", "name", "price"}
	values := []string{"1", "'Widget'", "19.99"}

	stmt := GenerateSingle("products", columnNames, values)

	expected := "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 19.99);"
	if stmt != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, stmt)
	}
}

func TestGenerateSingle_WithSpecialValues(t *testing.T) {
	columnNames := []string{"id", "name", "tags", "metadata"}
	values := []string{"1", "'Widget''s Best'", "(1,2,3)", "'{\"key\":\"value\"}'"}

	stmt := GenerateSingle("products", columnNames, values)

	expected := "INSERT INTO products (id, name, tags, metadata) VALUES (1, 'Widget''s Best', (1,2,3), '{\"key\":\"value\"}');"
	if stmt != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, stmt)
	}
}
