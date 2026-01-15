package parser

import (
	"io"
	"strings"
	"testing"
)

func TestNewCSVProcessor(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		skipHeader bool
		wantErr    bool
	}{
		{
			name:       "without header skip",
			input:      "1,test,value\n2,test2,value2\n",
			skipHeader: false,
		},
		{
			name:       "with header skip",
			input:      "id,name,value\n1,test,value\n",
			skipHeader: true,
		},
		{
			name:       "empty file with skip header",
			input:      "",
			skipHeader: true,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			proc, err := NewCSVProcessor(reader, tt.skipHeader, ',')

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

			if proc == nil {
				t.Error("expected processor, got nil")
			}
		})
	}
}

func TestCSVProcessor_Next(t *testing.T) {
	input := "1,Widget,19.99\n2,Gadget,29.99\n3,Thing,39.99\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	expected := [][]string{
		{"1", "Widget", "19.99"},
		{"2", "Gadget", "29.99"},
		{"3", "Thing", "39.99"},
	}

	for i, exp := range expected {
		record, lineNum, err := proc.Next()
		if err != nil {
			t.Fatalf("unexpected error on row %d: %v", i, err)
		}

		if lineNum != i+1 {
			t.Errorf("row %d: expected lineNum %d, got %d", i, i+1, lineNum)
		}

		if len(record) != len(exp) {
			t.Fatalf("row %d: expected %d fields, got %d", i, len(exp), len(record))
		}

		for j, val := range record {
			if val != exp[j] {
				t.Errorf("row %d, field %d: expected %q, got %q", i, j, exp[j], val)
			}
		}
	}

	// Should return EOF on next call
	_, _, err = proc.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestCSVProcessor_SkipHeader(t *testing.T) {
	input := "id,name,price\n1,Widget,19.99\n2,Gadget,29.99\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, true, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	// First record should be data, not header
	record, lineNum, err := proc.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Line number should be 2 (header was line 1)
	if lineNum != 2 {
		t.Errorf("expected lineNum 2, got %d", lineNum)
	}

	if record[0] != "1" || record[1] != "Widget" {
		t.Errorf("expected first data row, got header or wrong data: %v", record)
	}
}

func TestCSVProcessor_QuotedFields(t *testing.T) {
	input := `1,"Widget Pro","A ""great"" product"` + "\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	record, _, err := proc.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"1", "Widget Pro", `A "great" product`}
	for i, exp := range expected {
		if record[i] != exp {
			t.Errorf("field %d: expected %q, got %q", i, exp, record[i])
		}
	}
}

func TestCSVProcessor_FieldsWithCommas(t *testing.T) {
	input := `1,"Widget, Pro",19.99` + "\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	record, _, err := proc.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(record) != 3 {
		t.Fatalf("expected 3 fields, got %d: %v", len(record), record)
	}

	if record[1] != "Widget, Pro" {
		t.Errorf("expected %q, got %q", "Widget, Pro", record[1])
	}
}

func TestCSVProcessor_JSONInField(t *testing.T) {
	input := `1,"Test","{""color"":""red"",""size"":10}"` + "\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	record, _, err := proc.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(record) != 3 {
		t.Fatalf("expected 3 fields, got %d: %v", len(record), record)
	}

	expectedJSON := `{"color":"red","size":10}`
	if record[2] != expectedJSON {
		t.Errorf("expected %q, got %q", expectedJSON, record[2])
	}
}

func TestCSVProcessor_LineNum(t *testing.T) {
	input := "a\nb\nc\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	if proc.LineNum() != 0 {
		t.Errorf("initial LineNum should be 0, got %d", proc.LineNum())
	}

	_, _, _ = proc.Next()
	if proc.LineNum() != 1 {
		t.Errorf("after first read, LineNum should be 1, got %d", proc.LineNum())
	}

	_, _, _ = proc.Next()
	if proc.LineNum() != 2 {
		t.Errorf("after second read, LineNum should be 2, got %d", proc.LineNum())
	}
}

func TestCSVProcessor_TSV(t *testing.T) {
	input := "1\tWidget\t19.99\n2\tGadget\t29.99\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, '\t')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	expected := [][]string{
		{"1", "Widget", "19.99"},
		{"2", "Gadget", "29.99"},
	}

	for i, exp := range expected {
		record, lineNum, err := proc.Next()
		if err != nil {
			t.Fatalf("unexpected error on row %d: %v", i, err)
		}

		if lineNum != i+1 {
			t.Errorf("row %d: expected lineNum %d, got %d", i, i+1, lineNum)
		}

		if len(record) != len(exp) {
			t.Fatalf("row %d: expected %d fields, got %d", i, len(exp), len(record))
		}

		for j, val := range record {
			if val != exp[j] {
				t.Errorf("row %d, field %d: expected %q, got %q", i, j, exp[j], val)
			}
		}
	}

	_, _, err = proc.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestCSVProcessor_AutoID(t *testing.T) {
	// First column is not a number, so auto-ID should be enabled
	input := "Widget,19.99\nGadget,29.99\nThing,39.99\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	expected := [][]string{
		{"1", "Widget", "19.99"},
		{"2", "Gadget", "29.99"},
		{"3", "Thing", "39.99"},
	}

	for i, exp := range expected {
		record, lineNum, err := proc.Next()
		if err != nil {
			t.Fatalf("unexpected error on row %d: %v", i, err)
		}

		if lineNum != i+1 {
			t.Errorf("row %d: expected lineNum %d, got %d", i, i+1, lineNum)
		}

		if len(record) != len(exp) {
			t.Fatalf("row %d: expected %d fields, got %d: %v", i, len(exp), len(record), record)
		}

		for j, val := range record {
			if val != exp[j] {
				t.Errorf("row %d, field %d: expected %q, got %q", i, j, exp[j], val)
			}
		}
	}

	_, _, err = proc.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestCSVProcessor_AutoID_WithHeader(t *testing.T) {
	// First column of data row is not a number, so auto-ID should be enabled
	input := "name,price\nWidget,19.99\nGadget,29.99\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, true, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	expected := [][]string{
		{"1", "Widget", "19.99"},
		{"2", "Gadget", "29.99"},
	}

	for i, exp := range expected {
		record, _, err := proc.Next()
		if err != nil {
			t.Fatalf("unexpected error on row %d: %v", i, err)
		}

		if len(record) != len(exp) {
			t.Fatalf("row %d: expected %d fields, got %d: %v", i, len(exp), len(record), record)
		}

		for j, val := range record {
			if val != exp[j] {
				t.Errorf("row %d, field %d: expected %q, got %q", i, j, exp[j], val)
			}
		}
	}
}

func TestCSVProcessor_NoAutoID_WhenFirstColumnIsNumber(t *testing.T) {
	// First column is a number, so auto-ID should NOT be enabled
	input := "100,Widget,19.99\n200,Gadget,29.99\n"
	reader := strings.NewReader(input)

	proc, err := NewCSVProcessor(reader, false, ',')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	expected := [][]string{
		{"100", "Widget", "19.99"},
		{"200", "Gadget", "29.99"},
	}

	for i, exp := range expected {
		record, _, err := proc.Next()
		if err != nil {
			t.Fatalf("unexpected error on row %d: %v", i, err)
		}

		if len(record) != len(exp) {
			t.Fatalf("row %d: expected %d fields, got %d: %v", i, len(exp), len(record), record)
		}

		for j, val := range record {
			if val != exp[j] {
				t.Errorf("row %d, field %d: expected %q, got %q", i, j, exp[j], val)
			}
		}
	}
}

func TestCSVProcessor_TSV_AddressData(t *testing.T) {
	// Real-world TSV address data with header and one data row
	// First column (record_hash) is non-numeric, so auto-ID should be enabled
	input := "record_hash\tnumber\tstreet\tstreet2\tcity\tstate\tcountry\tcounty\tlatitude\tlongitude\tpostal_code\taddress\tfull_address\tcreated_date\tis_unincorporated\tuploaded_to_es_at\tsource\n" +
		"e50574c8da4b33623f8d0748f84cd803\t36320\tHWY 74 HIGHWAY\t\tGEISMAR\tLA\tUS\tASCENSION PARISH\t30.256591\t-90.995407\t70734\t36320 HWY 74 HIGHWAY\t36320 HWY 74 HIGHWAY, GEISMAR, LA, 70734, US\t2025-07-22\ttrue\t2025-07-26T19:09:18Z\toriginal\n"

	reader := strings.NewReader(input)
	proc, err := NewCSVProcessor(reader, true, '\t')
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}

	record, lineNum, err := proc.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Line number should be 2 (header was line 1)
	if lineNum != 2 {
		t.Errorf("expected lineNum 2, got %d", lineNum)
	}

	// With auto-ID enabled, we should have 18 fields (1 generated ID + 17 original)
	if len(record) != 18 {
		t.Fatalf("expected 18 fields (auto-ID + 17 original), got %d: %v", len(record), record)
	}

	// First field should be auto-generated ID "1"
	if record[0] != "1" {
		t.Errorf("expected auto-generated ID '1', got %q", record[0])
	}

	// Verify specific fields
	expectedFields := map[int]string{
		0:  "1",                                            // auto-ID
		1:  "e50574c8da4b33623f8d0748f84cd803",             // record_hash
		2:  "36320",                                        // number
		3:  "HWY 74 HIGHWAY",                               // street
		4:  "",                                             // street2 (empty)
		5:  "GEISMAR",                                      // city
		6:  "LA",                                           // state
		7:  "US",                                           // country
		8:  "ASCENSION PARISH",                             // county
		9:  "30.256591",                                    // latitude
		10: "-90.995407",                                   // longitude
		11: "70734",                                        // postal_code
		12: "36320 HWY 74 HIGHWAY",                         // address
		13: "36320 HWY 74 HIGHWAY, GEISMAR, LA, 70734, US", // full_address
		14: "2025-07-22",                                   // created_date
		15: "true",                                         // is_unincorporated
		16: "2025-07-26T19:09:18Z",                         // uploaded_to_es_at
		17: "original",                                     // source
	}

	for idx, expected := range expectedFields {
		if record[idx] != expected {
			t.Errorf("field %d: expected %q, got %q", idx, expected, record[idx])
		}
	}

	// Should return EOF on next call
	_, _, err = proc.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}
