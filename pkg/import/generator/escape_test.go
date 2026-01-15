package generator

import "testing"

func TestEscapeSQL(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"with'quote", "with\\'quote"},
		{"multiple'quotes'here", "multiple\\'quotes\\'here"},
		{"", ""},
		{"no quotes at all", "no quotes at all"},
		{"'", "\\'"},
		{"''", "\\'\\'"},
		{"it's a test", "it\\'s a test"},
		{"O'Brien's data", "O\\'Brien\\'s data"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := EscapeSQL(tt.input)
			if got != tt.want {
				t.Errorf("EscapeSQL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestQuoteString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "'simple'"},
		{"with'quote", "'with\\'quote'"},
		{"", "''"},
		{"hello world", "'hello world'"},
		{"it's", "'it\\'s'"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := QuoteString(tt.input)
			if got != tt.want {
				t.Errorf("QuoteString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
