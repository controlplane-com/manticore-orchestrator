package parser

import (
	"strings"
	"testing"

	"github.com/cuppojoe/csv-to-manticore/pkg/config"
)

func TestConvertRow(t *testing.T) {
	// Columns should NOT include id - it's handled automatically as the first CSV column
	columns := []config.Column{
		{Name: "name", Type: config.TypeFieldString},
		{Name: "price", Type: config.TypeAttrFloat},
		{Name: "active", Type: config.TypeAttrBool},
		{Name: "tags", Type: config.TypeAttrMulti},
	}

	tests := []struct {
		name    string
		record  []string // First column is ID
		wantID  string
		want    []string
		wantErr bool
	}{
		{
			name:   "valid row",
			record: []string{"1", "Widget", "19.99", "true", "1,2,3"},
			wantID: "1",
			want:   []string{"'Widget'", "19.99", "1", "(1,2,3)"},
		},
		{
			name:   "with quotes in string",
			record: []string{"2", "Widget's Best", "29.99", "false", "4,5"},
			wantID: "2",
			want:   []string{"'Widget\\'s Best'", "29.99", "0", "(4,5)"},
		},
		{
			name:    "wrong column count",
			record:  []string{"1", "Widget"},
			wantErr: true,
		},
		{
			name:    "empty record",
			record:  []string{},
			wantErr: true,
		},
		{
			name:    "invalid ID",
			record:  []string{"abc", "Widget", "19.99", "true", "1,2,3"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, got, err := ConvertRow(tt.record, columns, 1)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if gotID != tt.wantID {
				t.Errorf("expected ID %q, got %q", tt.wantID, gotID)
			}

			if len(got) != len(tt.want) {
				t.Fatalf("expected %d values, got %d", len(tt.want), len(got))
			}

			for i, v := range got {
				if v != tt.want[i] {
					t.Errorf("value[%d]: expected %q, got %q", i, tt.want[i], v)
				}
			}
		})
	}
}

func TestConvertRow_AllTypes(t *testing.T) {
	// Columns should NOT include id - it's handled automatically as the first CSV column
	columns := []config.Column{
		{Name: "c1", Type: config.TypeField},
		{Name: "c2", Type: config.TypeFieldString},
		{Name: "c3", Type: config.TypeAttrUint},
		{Name: "c4", Type: config.TypeAttrBigint},
		{Name: "c5", Type: config.TypeAttrFloat},
		{Name: "c6", Type: config.TypeAttrBool},
		{Name: "c7", Type: config.TypeAttrTimestamp},
		{Name: "c8", Type: config.TypeAttrString},
		{Name: "c9", Type: config.TypeAttrMulti},
		{Name: "c10", Type: config.TypeAttrMulti64},
		{Name: "c11", Type: config.TypeAttrJSON},
	}

	// First column is ID, followed by data columns
	record := []string{
		"12345", // ID
		"text field",
		"text string",
		"42",
		"9223372036854775807",
		"3.14159",
		"yes",
		"1705276800",
		"plain string",
		"1,2,3",
		"100,200,300",
		`{"key":"value"}`,
	}

	gotID, got, err := ConvertRow(record, columns, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotID != "12345" {
		t.Errorf("expected ID '12345', got %q", gotID)
	}

	expected := []string{
		"'text field'",
		"'text string'",
		"42",
		"9223372036854775807",
		"3.14159",
		"1",
		"1705276800",
		"'plain string'",
		"(1,2,3)",
		"(100,200,300)",
		`'{"key":"value"}'`,
	}

	for i, v := range got {
		if v != expected[i] {
			t.Errorf("value[%d]: expected %q, got %q", i, expected[i], v)
		}
	}
}

func TestFormatUint(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"0", "0", false},
		{"42", "42", false},
		{"4294967295", "4294967295", false}, // max uint32
		{"", "0", false},
		{"-1", "", true},
		{"4294967296", "", true}, // overflow uint32
		{"abc", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := formatUint(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestFormatBigint(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"0", "0", false},
		{"42", "42", false},
		{"-42", "-42", false},
		{"9223372036854775807", "9223372036854775807", false},   // max int64
		{"-9223372036854775808", "-9223372036854775808", false}, // min int64
		{"", "0", false},
		{"abc", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := formatBigint(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"0", "0", false},
		{"3.14", "3.14", false},
		{"-3.14", "-3.14", false},
		{"1.5e10", "15000000000", false},
		{"", "0", false},
		{"abc", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := formatFloat(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestFormatBool(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		// True values
		{"1", "1", false},
		{"true", "1", false},
		{"TRUE", "1", false},
		{"True", "1", false},
		{"yes", "1", false},
		{"YES", "1", false},
		{"on", "1", false},
		{"t", "1", false},
		{"y", "1", false},
		// False values
		{"0", "0", false},
		{"false", "0", false},
		{"FALSE", "0", false},
		{"False", "0", false},
		{"no", "0", false},
		{"off", "0", false},
		{"f", "0", false},
		{"n", "0", false},
		{"", "0", false},
		// Invalid
		{"maybe", "", true},
		{"2", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := formatBool(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestFormatTimestamp(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		// Unix timestamp
		{"1705276800", "1705276800", false},
		{"0", "0", false},
		// Date formats
		{"2024-01-15", "1705276800", false},
		{"2024-01-15 00:00:00", "1705276800", false},
		{"2024-01-15T00:00:00", "1705276800", false},
		{"2024-01-15T00:00:00Z", "1705276800", false},
		// Empty
		{"", "0", false},
		// Invalid
		{"not-a-date", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := formatTimestamp(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestFormatMVA(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		bits    int
		want    string
		wantErr bool
	}{
		// Comma-separated
		{"comma separated", "1,2,3", 32, "(1,2,3)", false},
		{"comma with spaces", "1, 2, 3", 32, "(1,2,3)", false},
		// Space-separated
		{"space separated", "1 2 3", 32, "(1,2,3)", false},
		// JSON array
		{"json array", "[1,2,3]", 32, "(1,2,3)", false},
		{"json array 64", "[100,200,300]", 64, "(100,200,300)", false},
		// Empty
		{"empty", "", 32, "()", false},
		// Single value
		{"single value", "42", 32, "(42)", false},
		// 64-bit
		{"64-bit values", "9223372036854775807,-9223372036854775808", 64, "(9223372036854775807,-9223372036854775808)", false},
		// Errors
		{"invalid number", "1,abc,3", 32, "", true},
		{"invalid json", "[1,2,", 32, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatMVA(tt.input, tt.bits)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestFormatJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"object", `{"key":"value"}`, `'{"key":"value"}'`, false},
		{"array", `[1,2,3]`, `'[1,2,3]'`, false},
		{"nested", `{"nested":{"key":"value"}}`, `'{"nested":{"key":"value"}}'`, false},
		{"with quotes", `{"name":"it's"}`, `'{"name":"it\'s"}'`, false},
		{"empty", "", "'{}'", false},
		{"whitespace", "  ", "'{}'", false},
		{"invalid json", `{"key":}`, "", true},
		{"not json", `just text`, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatJSON(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestParseError(t *testing.T) {
	err := &ParseError{
		Line:   42,
		Column: "price",
		Reason: "invalid float",
	}

	msg := err.Error()
	if !strings.Contains(msg, "line 42") {
		t.Errorf("error message should contain line number: %s", msg)
	}
	if !strings.Contains(msg, `"price"`) {
		t.Errorf("error message should contain column name: %s", msg)
	}
	if !strings.Contains(msg, "invalid float") {
		t.Errorf("error message should contain reason: %s", msg)
	}
}

func TestConvertRow_AddressData(t *testing.T) {
	// Config for address data (16 columns after auto-ID is prepended)
	columns := []config.Column{
		{Name: "record_hash", Type: config.TypeAttrString},
		{Name: "number", Type: config.TypeAttrUint},
		{Name: "street", Type: config.TypeAttrString},
		{Name: "street2", Type: config.TypeAttrString},
		{Name: "city", Type: config.TypeAttrString},
		{Name: "state", Type: config.TypeAttrString},
		{Name: "country", Type: config.TypeAttrString},
		{Name: "county", Type: config.TypeAttrString},
		{Name: "latitude", Type: config.TypeAttrFloat},
		{Name: "longitude", Type: config.TypeAttrFloat},
		{Name: "postal_code", Type: config.TypeAttrString},
		{Name: "address", Type: config.TypeAttrString},
		{Name: "full_address", Type: config.TypeAttrString},
		{Name: "created_date", Type: config.TypeAttrTimestamp},
		{Name: "is_unincorporated", Type: config.TypeAttrBool},
		{Name: "uploaded_to_es_at", Type: config.TypeAttrTimestamp},
		{Name: "source", Type: config.TypeAttrString},
	}

	// Record as it would come from CSVProcessor with auto-ID prepended
	record := []string{
		"1",                                // auto-generated ID
		"e50574c8da4b33623f8d0748f84cd803", // record_hash
		"36320",                            // number
		"HWY 74 HIGHWAY",                   // street
		"",                                 // street2 (empty)
		"GEISMAR",                          // city
		"LA",                               // state
		"US",                               // country
		"ASCENSION PARISH",                 // county
		"30.256591",                        // latitude
		"-90.995407",                       // longitude
		"70734",                            // postal_code
		"36320 HWY 74 HIGHWAY",             // address
		"36320 HWY 74 HIGHWAY, GEISMAR, LA, 70734, US", // full_address
		"2025-07-22",           // created_date
		"true",                 // is_unincorporated
		"2025-07-26T19:09:18Z", // uploaded_to_es_at
		"original",             // source
	}

	gotID, got, err := ConvertRow(record, columns, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify ID
	if gotID != "1" {
		t.Errorf("expected ID '1', got %q", gotID)
	}

	// Verify converted values
	expected := []string{
		"'e50574c8da4b33623f8d0748f84cd803'", // record_hash (quoted string)
		"36320",                              // number (uint)
		"'HWY 74 HIGHWAY'",                   // street (quoted string)
		"''",                                 // street2 (empty string)
		"'GEISMAR'",                          // city
		"'LA'",                               // state
		"'US'",                               // country
		"'ASCENSION PARISH'",                 // county
		"30.256591",                          // latitude (float)
		"-90.995407",                         // longitude (float)
		"'70734'",                            // postal_code (string, not uint)
		"'36320 HWY 74 HIGHWAY'",             // address
		"'36320 HWY 74 HIGHWAY, GEISMAR, LA, 70734, US'", // full_address
		"1753142400", // created_date (2025-07-22 as Unix timestamp)
		"1",          // is_unincorporated (true -> 1)
		"1753556958", // uploaded_to_es_at (2025-07-26T19:09:18Z as Unix timestamp)
		"'original'", // source
	}

	if len(got) != len(expected) {
		t.Fatalf("expected %d values, got %d", len(expected), len(got))
	}

	for i, v := range got {
		if v != expected[i] {
			t.Errorf("value[%d] (%s): expected %q, got %q", i, columns[i].Name, expected[i], v)
		}
	}
}
