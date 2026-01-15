package manticore

import "testing"

// TestParseLocalsFromTableCreateStatement tests the parseLocalsFromTableCreateStatement function
func TestParseLocalsFromTableCreateStatement(t *testing.T) {
	createStmt := "CREATE TABLE addresses_full type='distributed' local='addresses_full_main_a' local='addresses_full_delta'"

	locals, err := parseLocalsFromTableCreateStatement(createStmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"addresses_full_main_a", "addresses_full_delta"}
	if len(locals) != len(expected) {
		t.Fatalf("expected %d locals, got %d", len(expected), len(locals))
	}

	for i, local := range locals {
		if local != expected[i] {
			t.Errorf("expected locals[%d] = %q, got %q", i, expected[i], local)
		}
	}
}
