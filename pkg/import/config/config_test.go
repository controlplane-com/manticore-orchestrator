package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			json: `{"columns":[{"name":"id","type":"attr_bigint"},{"name":"title","type":"field"}]}`,
		},
		{
			name: "all types",
			json: `{"columns":[
				{"name":"id","type":"attr_bigint"},
				{"name":"title","type":"field"},
				{"name":"content","type":"field_string"},
				{"name":"count","type":"attr_uint"},
				{"name":"price","type":"attr_float"},
				{"name":"active","type":"attr_bool"},
				{"name":"created","type":"attr_timestamp"},
				{"name":"author","type":"attr_string"},
				{"name":"tags","type":"attr_multi"},
				{"name":"categories","type":"attr_multi_64"},
				{"name":"metadata","type":"attr_json"}
			]}`,
		},
		{
			name:    "empty columns",
			json:    `{"columns":[]}`,
			wantErr: true,
			errMsg:  "no columns defined",
		},
		{
			name:    "missing columns",
			json:    `{}`,
			wantErr: true,
			errMsg:  "no columns defined",
		},
		{
			name:    "empty column name",
			json:    `{"columns":[{"name":"","type":"field"}]}`,
			wantErr: true,
			errMsg:  "empty name",
		},
		{
			name:    "unknown type",
			json:    `{"columns":[{"name":"id","type":"invalid_type"}]}`,
			wantErr: true,
			errMsg:  "unknown type",
		},
		{
			name:    "invalid json",
			json:    `{"columns":[}`,
			wantErr: true,
			errMsg:  "parsing config JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write config to temp file
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.json")
			if err := os.WriteFile(configPath, []byte(tt.json), 0644); err != nil {
				t.Fatalf("failed to write temp config: %v", err)
			}

			cfg, err := LoadConfig(configPath)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errMsg)
					return
				}
				if tt.errMsg != "" && !containsString(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if cfg == nil {
				t.Error("expected config, got nil")
			}
		})
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.json")
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}

func TestConfig_ColumnNames(t *testing.T) {
	cfg := &Config{
		Columns: []Column{
			{Name: "id", Type: TypeAttrBigint},
			{Name: "title", Type: TypeField},
			{Name: "price", Type: TypeAttrFloat},
		},
	}

	names := cfg.ColumnNames()

	expected := []string{"id", "title", "price"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected name[%d] = %q, got %q", i, expected[i], name)
		}
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
