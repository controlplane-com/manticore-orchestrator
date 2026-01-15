package generator

import (
	"encoding/json"
	"strings"

	"github.com/cuppojoe/csv-to-manticore/pkg/config"
)

// BulkGenerator generates NDJSON for Manticore's bulk HTTP API.
// Each row is formatted as: {"insert":{"index":"table","id":123,"doc":{...}}}
type BulkGenerator struct {
	table     string
	columns   []config.Column
	batchSize int
	buffer    strings.Builder
	count     int
}

// NewBulkGenerator creates a new NDJSON bulk generator.
// If batchSize is 1 or less, each row produces its own NDJSON batch.
// If batchSize is greater than 1, rows are accumulated before returning.
func NewBulkGenerator(table string, columns []config.Column, batchSize int) *BulkGenerator {
	if batchSize < 1 {
		batchSize = 1
	}

	return &BulkGenerator{
		table:     table,
		columns:   columns,
		batchSize: batchSize,
	}
}

// AddRow adds a row with ID and JSON-native values to the generator.
// Returns NDJSON batch if the batch is full, otherwise returns empty string.
// Values should be native Go types (string, int64, float64, bool, []int64, map[string]interface{}).
func (g *BulkGenerator) AddRow(id int64, values []interface{}) string {
	doc := make(map[string]interface{})
	for i, col := range g.columns {
		if i < len(values) && values[i] != nil {
			doc[col.Name] = values[i]
		}
	}

	row := map[string]interface{}{
		"insert": map[string]interface{}{
			"index": g.table,
			"id":    id,
			"doc":   doc,
		},
	}

	jsonBytes, err := json.Marshal(row)
	if err != nil {
		// This shouldn't happen with basic types, but skip row if it does
		return ""
	}

	g.buffer.Write(jsonBytes)
	g.buffer.WriteByte('\n')
	g.count++

	if g.count >= g.batchSize {
		return g.Flush()
	}
	return ""
}

// Flush returns any accumulated NDJSON and resets the buffer.
// Returns empty string if there are no pending rows.
func (g *BulkGenerator) Flush() string {
	if g.count == 0 {
		return ""
	}
	result := g.buffer.String()
	g.buffer.Reset()
	g.count = 0
	return result
}

// Count returns the number of rows currently in the buffer.
func (g *BulkGenerator) Count() int {
	return g.count
}
