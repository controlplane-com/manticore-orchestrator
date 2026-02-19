package indexer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConvertBool(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"t", "1"},
		{"T", "1"},
		{"true", "1"},
		{"True", "1"},
		{"TRUE", "1"},
		{"1", "1"},
		{"yes", "1"},
		{"f", "0"},
		{"false", "0"},
		{"False", "0"},
		{"0", "0"},
		{"no", "0"},
		{"", "0"},
		{" t ", "1"},
		{" f ", "0"},
	}

	for _, tt := range tests {
		got := convertBool(tt.input)
		if got != tt.expected {
			t.Errorf("convertBool(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestDetectDelimiter(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"data.tsv", "\t"},
		{"data.TSV", "\t"},
		{"data.csv", ","},
		{"data.CSV", ","},
		{"data.txt", ","},
		{"/path/to/file.tsv", "\t"},
		{"/path/to/file.csv", ","},
	}

	for _, tt := range tests {
		got := detectDelimiter(tt.path)
		if got != tt.expected {
			t.Errorf("detectDelimiter(%q) = %q, want %q", tt.path, got, tt.expected)
		}
	}
}

func TestCsvHasHeader_TSV(t *testing.T) {
	// TSV with header (first field is non-numeric)
	tsvWithHeader := "record_hash\tnumber\tstreet\n" +
		"abc123\t42\tMain St\n"

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "data.tsv")
	if err := os.WriteFile(path, []byte(tsvWithHeader), 0644); err != nil {
		t.Fatal(err)
	}

	hasHeader, err := csvHasHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	if !hasHeader {
		t.Error("expected TSV with header to be detected as having a header")
	}

	// TSV without header (first field is numeric)
	tsvNoHeader := "12345\t42\tMain St\n" +
		"67890\t99\tElm St\n"

	path2 := filepath.Join(tmpDir, "noheader.tsv")
	if err := os.WriteFile(path2, []byte(tsvNoHeader), 0644); err != nil {
		t.Fatal(err)
	}

	hasHeader2, err := csvHasHeader(path2)
	if err != nil {
		t.Fatal(err)
	}
	if hasHeader2 {
		t.Error("expected TSV without header to be detected as not having a header")
	}
}

func TestCsvHasHeader_CSV(t *testing.T) {
	csvWithHeader := "name,age,city\nAlice,30,Portland\n"

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "data.csv")
	if err := os.WriteFile(path, []byte(csvWithHeader), 0644); err != nil {
		t.Fatal(err)
	}

	hasHeader, err := csvHasHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	if !hasHeader {
		t.Error("expected CSV with header to be detected as having a header")
	}
}

func TestPreprocessToTSV_AddressesSchema(t *testing.T) {
	// Simulates the user's addresses_us schema with TSV source data
	// Columns match their YAML config (created_at as attr_string, updated_at as attr_timestamp)
	columns := []Column{
		{Name: "record_hash", Type: "attr_string"},
		{Name: "number", Type: "attr_string"},
		{Name: "street", Type: "field"},
		{Name: "street2", Type: "field"},
		{Name: "city", Type: "field"},
		{Name: "state", Type: "attr_string"},
		{Name: "country", Type: "attr_string"},
		{Name: "county", Type: "field"},
		{Name: "latitude", Type: "attr_float"},
		{Name: "longitude", Type: "attr_float"},
		{Name: "postal_code", Type: "field"},
		{Name: "address", Type: "field"},
		{Name: "full_address", Type: "field"},
		{Name: "created_at", Type: "attr_string"},
		{Name: "is_unincorporated", Type: "attr_bool"},
		{Name: "source", Type: "attr_string"},
		{Name: "updated_at", Type: "attr_timestamp"},
	}

	// TSV source with header + 2 data rows matching the schema
	header := strings.Join([]string{
		"record_hash", "number", "street", "street2", "city", "state", "country",
		"county", "latitude", "longitude", "postal_code", "address", "full_address",
		"created_at", "is_unincorporated", "source", "updated_at",
	}, "\t")

	row1 := strings.Join([]string{
		"abc123", "123", "Main St", "", "Portland", "OR", "US",
		"Multnomah", "45.5231", "-122.6765", "97201", "123 Main St", "123 Main St Portland OR 97201",
		"2025-09-23 20:09:50.696333", "t", "geocoder", "",
	}, "\t")

	row2 := strings.Join([]string{
		"def456", "456", "Elm St", "Apt 2", "Salem", "OR", "US",
		"Marion", "44.9429", "-123.0351", "97301", "456 Elm St", "456 Elm St Apt 2 Salem OR 97301",
		"2025-10-01 12:00:00", "f", "manual", "",
	}, "\t")

	sourceData := header + "\n" + row1 + "\n" + row2 + "\n"

	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "addresses.tsv")
	dstPath := filepath.Join(tmpDir, "output.tsv")

	if err := os.WriteFile(srcPath, []byte(sourceData), 0644); err != nil {
		t.Fatal(err)
	}

	// Run preprocessor with hasHeader=true (as would be set by auto-detect or config)
	if err := preprocessToTSV(srcPath, dstPath, true, columns); err != nil {
		t.Fatalf("preprocessToTSV failed: %v", err)
	}

	// Read output
	output, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	// Should have exactly 2 lines (header skipped)
	if len(lines) != 2 {
		t.Fatalf("expected 2 output lines, got %d:\n%s", len(lines), string(output))
	}

	// Verify first line
	fields1 := strings.Split(lines[0], "\t")

	// First field should be doc ID "0"
	if fields1[0] != "0" {
		t.Errorf("expected doc ID 0, got %q", fields1[0])
	}

	// Total fields: 1 (docID) + 17 (columns) = 18
	if len(fields1) != 18 {
		t.Errorf("expected 18 fields, got %d: %v", len(fields1), fields1)
	}

	// Check column order preservation: record_hash should be field[1]
	if fields1[1] != "abc123" {
		t.Errorf("expected record_hash='abc123' at index 1, got %q", fields1[1])
	}

	// Check street (field type) is at index 3
	if fields1[3] != "Main St" {
		t.Errorf("expected street='Main St' at index 3, got %q", fields1[3])
	}

	// Check latitude (attr_float) preserved at index 9
	if fields1[9] != "45.5231" {
		t.Errorf("expected latitude='45.5231' at index 9, got %q", fields1[9])
	}

	// Check created_at (attr_string) passed through as-is at index 14
	if fields1[14] != "2025-09-23 20:09:50.696333" {
		t.Errorf("expected created_at passed through as-is, got %q", fields1[14])
	}

	// Check is_unincorporated (attr_bool) converted: "t" -> "1" at index 15
	if fields1[15] != "1" {
		t.Errorf("expected is_unincorporated 't' converted to '1', got %q", fields1[15])
	}

	// Verify second line
	fields2 := strings.Split(lines[1], "\t")

	// Doc ID should be "1"
	if fields2[0] != "1" {
		t.Errorf("expected doc ID 1, got %q", fields2[0])
	}

	// Check is_unincorporated (attr_bool) converted: "f" -> "0" at index 15
	if fields2[15] != "0" {
		t.Errorf("expected is_unincorporated 'f' converted to '0', got %q", fields2[15])
	}

	// Check updated_at (attr_timestamp) — trailing empty field may or may not
	// be present depending on how the TSV was split, so just verify field count
	// is at least 17 (docID + 16 non-empty columns)
	if len(fields2) < 17 {
		t.Errorf("expected at least 17 fields in row 2, got %d", len(fields2))
	}
}

func TestPreprocessToTSV_CSV(t *testing.T) {
	// Test CSV input (comma-delimited) with header
	columns := []Column{
		{Name: "name", Type: "field"},
		{Name: "active", Type: "attr_bool"},
		{Name: "score", Type: "attr_float"},
	}

	sourceData := "name,active,score\nAlice,true,9.5\nBob,false,7.2\n"

	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "data.csv")
	dstPath := filepath.Join(tmpDir, "output.tsv")

	if err := os.WriteFile(srcPath, []byte(sourceData), 0644); err != nil {
		t.Fatal(err)
	}

	if err := preprocessToTSV(srcPath, dstPath, true, columns); err != nil {
		t.Fatalf("preprocessToTSV failed: %v", err)
	}

	output, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 output lines, got %d", len(lines))
	}

	// Output should be tab-delimited regardless of input format
	fields := strings.Split(lines[0], "\t")
	if fields[0] != "0" {
		t.Errorf("expected doc ID 0, got %q", fields[0])
	}
	if fields[1] != "Alice" {
		t.Errorf("expected name='Alice', got %q", fields[1])
	}
	// "true" -> "1"
	if fields[2] != "1" {
		t.Errorf("expected active 'true' converted to '1', got %q", fields[2])
	}
	if fields[3] != "9.5" {
		t.Errorf("expected score='9.5', got %q", fields[3])
	}
}

func TestPreprocessToTSV_NoHeader(t *testing.T) {
	columns := []Column{
		{Name: "id", Type: "attr_uint"},
		{Name: "active", Type: "attr_bool"},
	}

	// No header — first row is data
	sourceData := "100\tt\n200\tf\n"

	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "data.tsv")
	dstPath := filepath.Join(tmpDir, "output.tsv")

	if err := os.WriteFile(srcPath, []byte(sourceData), 0644); err != nil {
		t.Fatal(err)
	}

	if err := preprocessToTSV(srcPath, dstPath, false, columns); err != nil {
		t.Fatalf("preprocessToTSV failed: %v", err)
	}

	output, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	// Both rows should be included (no header skip)
	if len(lines) != 2 {
		t.Fatalf("expected 2 output lines, got %d", len(lines))
	}

	fields := strings.Split(lines[0], "\t")
	if fields[1] != "100" {
		t.Errorf("expected first data field '100', got %q", fields[1])
	}
	if fields[2] != "1" {
		t.Errorf("expected bool 't' converted to '1', got %q", fields[2])
	}
}
