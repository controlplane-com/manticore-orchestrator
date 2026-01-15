package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config represents the column mapping configuration for CSV to Manticore conversion.
type Config struct {
	Columns []Column `json:"columns"`
}

// LoadConfig reads and parses a JSON configuration file from the given path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config JSON: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	if len(c.Columns) == 0 {
		return fmt.Errorf("config error: no columns defined")
	}

	for i, col := range c.Columns {
		if col.Name == "" {
			return fmt.Errorf("config error: column %d has empty name", i)
		}

		if _, ok := ValidTypes[string(col.Type)]; !ok {
			return fmt.Errorf("config error: column %q has unknown type %q", col.Name, col.Type)
		}
	}

	return nil
}

// ColumnNames returns a slice of all column names in order.
func (c *Config) ColumnNames() []string {
	names := make([]string, len(c.Columns))
	for i, col := range c.Columns {
		names[i] = col.Name
	}
	return names
}
