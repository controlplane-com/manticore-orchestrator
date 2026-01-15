package actions

import (
	"testing"
)

func TestContext_MainTableName(t *testing.T) {
	tests := []struct {
		name     string
		dataset  string
		slot     string
		expected string
	}{
		{"slot a", "products", "a", "products_main_a"},
		{"slot b", "products", "b", "products_main_b"},
		{"different dataset", "addresses", "a", "addresses_main_a"},
		{"empty slot", "items", "", "items_main_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &Context{Dataset: tt.dataset}
			result := ctx.MainTableName(tt.slot)
			if result != tt.expected {
				t.Errorf("MainTableName(%q) = %q, want %q", tt.slot, result, tt.expected)
			}
		})
	}
}

func TestContext_DeltaTableName(t *testing.T) {
	tests := []struct {
		name     string
		dataset  string
		expected string
	}{
		{"products", "products", "products_delta"},
		{"addresses", "addresses", "addresses_delta"},
		{"empty dataset", "", "_delta"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &Context{Dataset: tt.dataset}
			result := ctx.DeltaTableName()
			if result != tt.expected {
				t.Errorf("DeltaTableName() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestContext_DistributedTableName(t *testing.T) {
	tests := []struct {
		name     string
		dataset  string
		expected string
	}{
		{"products", "products", "products"},
		{"addresses", "addresses", "addresses"},
		{"empty dataset", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &Context{Dataset: tt.dataset}
			result := ctx.DistributedTableName()
			if result != tt.expected {
				t.Errorf("DistributedTableName() = %q, want %q", result, tt.expected)
			}
		})
	}
}
