package manticore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetColumnType(t *testing.T) {
	tests := []struct {
		name     string
		typeStr  string
		expected ColumnType
	}{
		{"field type", "field", ColumnTypeField},
		{"field_string type", "field_string", ColumnTypeFieldString},
		{"attr_uint type", "attr_uint", ColumnTypeUint},
		{"attr_bigint type", "attr_bigint", ColumnTypeBigint},
		{"attr_float type", "attr_float", ColumnTypeFloat},
		{"attr_bool type", "attr_bool", ColumnTypeBool},
		{"attr_string type", "attr_string", ColumnTypeString},
		{"attr_timestamp type", "attr_timestamp", ColumnTypeTimestamp},
		{"attr_multi type", "attr_multi", ColumnTypeMulti},
		{"attr_multi_64 type", "attr_multi_64", ColumnTypeMulti64},
		{"attr_json type", "attr_json", ColumnTypeJSON},
		{"unknown type defaults to field", "unknown", ColumnTypeField},
		{"empty string defaults to field", "", ColumnTypeField},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetColumnType(tt.typeStr)
			if result != tt.expected {
				t.Errorf("GetColumnType(%q) = %v, want %v", tt.typeStr, result, tt.expected)
			}
		})
	}
}

func TestGetRTTypeName(t *testing.T) {
	tests := []struct {
		name       string
		columnType ColumnType
		expected   string
	}{
		{"field to text", ColumnTypeField, "text"},
		{"field_string to text", ColumnTypeFieldString, "text"},
		{"uint", ColumnTypeUint, "uint"},
		{"bigint", ColumnTypeBigint, "bigint"},
		{"float", ColumnTypeFloat, "float"},
		{"bool", ColumnTypeBool, "bool"},
		{"string", ColumnTypeString, "string"},
		{"timestamp", ColumnTypeTimestamp, "timestamp"},
		{"multi", ColumnTypeMulti, "multi"},
		{"multi64", ColumnTypeMulti64, "multi64"},
		{"json", ColumnTypeJSON, "json"},
		{"unknown defaults to text", ColumnType(999), "text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetRTTypeName(tt.columnType)
			if result != tt.expected {
				t.Errorf("GetRTTypeName(%v) = %q, want %q", tt.columnType, result, tt.expected)
			}
		})
	}
}

func TestExtractBaseTableName(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		expected  string
	}{
		{"main_a suffix", "addresses_main_a", "addresses"},
		{"main_b suffix", "addresses_main_b", "addresses"},
		{"delta suffix", "addresses_delta", "addresses"},
		{"no suffix passthrough", "addresses", "addresses"},
		{"complex name with main_a", "my_table_name_main_a", "my_table_name"},
		{"complex name with delta", "products_list_delta", "products_list"},
		{"empty string", "", ""},
		{"only suffix", "_main_a", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractBaseTableName(tt.tableName)
			if result != tt.expected {
				t.Errorf("ExtractBaseTableName(%q) = %q, want %q", tt.tableName, result, tt.expected)
			}
		})
	}
}

func TestGenerateCreateTableSQL(t *testing.T) {
	tests := []struct {
		name      string
		schema    *Schema
		tableName string
		expected  string
	}{
		{
			name: "simple schema with one column",
			schema: &Schema{
				Columns: []Column{
					{Name: "title", Type: "field"},
				},
			},
			tableName: "products",
			expected:  "CREATE TABLE IF NOT EXISTS products (id bigint, title text)",
		},
		{
			name: "schema with multiple columns",
			schema: &Schema{
				Columns: []Column{
					{Name: "title", Type: "field"},
					{Name: "price", Type: "attr_float"},
					{Name: "quantity", Type: "attr_uint"},
				},
			},
			tableName: "products",
			expected:  "CREATE TABLE IF NOT EXISTS products (id bigint, title text, price float, quantity uint)",
		},
		{
			name: "schema with all types",
			schema: &Schema{
				Columns: []Column{
					{Name: "content", Type: "field"},
					{Name: "name", Type: "attr_string"},
					{Name: "count", Type: "attr_bigint"},
					{Name: "active", Type: "attr_bool"},
					{Name: "created", Type: "attr_timestamp"},
					{Name: "data", Type: "attr_json"},
				},
			},
			tableName: "items",
			expected:  "CREATE TABLE IF NOT EXISTS items (id bigint, content text, name string, count bigint, active bool, created timestamp, data json)",
		},
		{
			name:      "empty schema",
			schema:    &Schema{Columns: []Column{}},
			tableName: "empty_table",
			expected:  "CREATE TABLE IF NOT EXISTS empty_table (id bigint)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.schema.GenerateCreateTableSQL(tt.tableName)
			if result != tt.expected {
				t.Errorf("GenerateCreateTableSQL(%q) = %q, want %q", tt.tableName, result, tt.expected)
			}
		})
	}
}

func TestCSVHasHeader(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expected    bool
		expectError bool
	}{
		{
			name:     "has header (text first field)",
			content:  "id,name,price\n1,Product A,10.99\n",
			expected: true,
		},
		{
			name:     "no header (numeric first field)",
			content:  "1,Product A,10.99\n2,Product B,20.99\n",
			expected: false,
		},
		{
			name:     "header with spaces",
			content:  "  id  ,name,price\n1,Product A,10.99\n",
			expected: true,
		},
		{
			name:        "empty file",
			content:     "",
			expectError: true,
		},
		{
			name:     "single empty line",
			content:  "\n",
			expected: false, // empty first line returns false
		},
		{
			name:     "negative number first field",
			content:  "-123,data\n",
			expected: false,
		},
		{
			name:     "float-like string is header",
			content:  "1.5,data\n", // ParseInt fails on float, so treated as header
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.csv")
			if err := os.WriteFile(tmpFile, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			result, err := CSVHasHeader(tmpFile)

			if tt.expectError {
				if err == nil {
					t.Errorf("CSVHasHeader() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("CSVHasHeader() unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("CSVHasHeader() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCSVHasHeader_FileNotFound(t *testing.T) {
	_, err := CSVHasHeader("/nonexistent/path/to/file.csv")
	if err == nil {
		t.Error("CSVHasHeader() expected error for nonexistent file, got nil")
	}
}

func TestSchemaRegistry(t *testing.T) {
	t.Run("NewSchemaRegistry creates empty registry", func(t *testing.T) {
		r := NewSchemaRegistry()
		if r == nil {
			t.Fatal("NewSchemaRegistry() returned nil")
		}
		if len(r.List()) != 0 {
			t.Errorf("New registry should be empty, got %d items", len(r.List()))
		}
	})

	t.Run("Get returns false for nonexistent schema", func(t *testing.T) {
		r := NewSchemaRegistry()
		_, ok := r.Get("nonexistent")
		if ok {
			t.Error("Get() should return false for nonexistent schema")
		}
	})

	t.Run("List returns sorted names", func(t *testing.T) {
		r := NewSchemaRegistry()
		r.schemas["zebra"] = &Schema{}
		r.schemas["apple"] = &Schema{}
		r.schemas["mango"] = &Schema{}

		names := r.List()
		if len(names) != 3 {
			t.Fatalf("List() returned %d items, want 3", len(names))
		}
		if names[0] != "apple" || names[1] != "mango" || names[2] != "zebra" {
			t.Errorf("List() not sorted: got %v", names)
		}
	})
}

func TestSchemaRegistry_LoadFromFile(t *testing.T) {
	t.Run("loads valid YAML schema", func(t *testing.T) {
		content := `
products:
  schema:
    columns:
      - name: title
        type: field
      - name: price
        type: attr_float
addresses:
  schema:
    columns:
      - name: street
        type: attr_string
      - name: zip
        type: attr_uint
`
		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "schema.yaml")
		if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}

		r := NewSchemaRegistry()
		if err := r.LoadFromFile(tmpFile); err != nil {
			t.Fatalf("LoadFromFile() error: %v", err)
		}

		// Check products schema
		products, ok := r.Get("products")
		if !ok {
			t.Fatal("products schema not found")
		}
		if len(products.Columns) != 2 {
			t.Errorf("products has %d columns, want 2", len(products.Columns))
		}
		if products.Columns[0].Name != "title" {
			t.Errorf("first column name = %q, want 'title'", products.Columns[0].Name)
		}

		// Check addresses schema
		addresses, ok := r.Get("addresses")
		if !ok {
			t.Fatal("addresses schema not found")
		}
		if len(addresses.Columns) != 2 {
			t.Errorf("addresses has %d columns, want 2", len(addresses.Columns))
		}

		// Check list is sorted
		names := r.List()
		if names[0] != "addresses" || names[1] != "products" {
			t.Errorf("List() = %v, want [addresses, products]", names)
		}
	})

	t.Run("returns error for nonexistent file", func(t *testing.T) {
		r := NewSchemaRegistry()
		err := r.LoadFromFile("/nonexistent/path/schema.yaml")
		if err == nil {
			t.Error("LoadFromFile() should return error for nonexistent file")
		}
	})

	t.Run("returns error for invalid YAML", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "invalid.yaml")
		if err := os.WriteFile(tmpFile, []byte("not: valid: yaml: ["), 0644); err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}

		r := NewSchemaRegistry()
		err := r.LoadFromFile(tmpFile)
		if err == nil {
			t.Error("LoadFromFile() should return error for invalid YAML")
		}
	})
}

func TestSchemaRegistry_GetForDerivedTable(t *testing.T) {
	r := NewSchemaRegistry()
	r.schemas["products"] = &Schema{
		Columns: []Column{{Name: "title", Type: "field"}},
	}

	tests := []struct {
		name      string
		tableName string
		wantOk    bool
	}{
		{"base name", "products", true},
		{"main_a suffix", "products_main_a", true},
		{"main_b suffix", "products_main_b", true},
		{"delta suffix", "products_delta", true},
		{"nonexistent base", "orders_main_a", false},
		{"nonexistent table", "orders", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, ok := r.GetForDerivedTable(tt.tableName)
			if ok != tt.wantOk {
				t.Errorf("GetForDerivedTable(%q) ok = %v, want %v", tt.tableName, ok, tt.wantOk)
			}
			if tt.wantOk && schema == nil {
				t.Errorf("GetForDerivedTable(%q) returned nil schema", tt.tableName)
			}
		})
	}
}

func TestSchema_JSONConfig(t *testing.T) {
	content := `
products:
  schema:
    columns:
      - name: title
        type: field
      - name: price
        type: attr_float
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "schema.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	r := NewSchemaRegistry()
	if err := r.LoadFromFile(tmpFile); err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	products, _ := r.Get("products")
	if products.JSONConfig == "" {
		t.Error("JSONConfig should not be empty")
	}

	// Verify it's valid JSON
	if products.JSONConfig[0] != '{' {
		t.Errorf("JSONConfig should be valid JSON, got: %s", products.JSONConfig)
	}
}

func TestColumn_ZeroValue(t *testing.T) {
	col := Column{}
	if col.Name != "" || col.Type != "" {
		t.Error("Zero Column should have empty fields")
	}
}

func TestSchema_ZeroValue(t *testing.T) {
	schema := Schema{}
	if schema.Columns != nil || schema.JSONConfig != "" {
		t.Error("Zero Schema should have zero values")
	}
}
