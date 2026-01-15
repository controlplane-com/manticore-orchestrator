package generator

import (
	"strings"

	"github.com/cuppojoe/csv-to-manticore/pkg/config"
)

// InsertGenerator generates SQL INSERT statements with optional batching.
type InsertGenerator struct {
	table       string
	columnNames []string
	batchSize   int
	batch       [][]string
}

// NewInsertGenerator creates a new INSERT statement generator.
// If batchSize is 1 or less, each row produces its own INSERT statement.
// If batchSize is greater than 1, rows are batched into multi-row INSERTs.
// The id column is automatically prepended to the column list.
func NewInsertGenerator(table string, columns []config.Column, batchSize int) *InsertGenerator {
	// Prepend "id" to column names
	names := make([]string, len(columns)+1)
	names[0] = "id"
	for i, col := range columns {
		names[i+1] = col.Name
	}

	if batchSize < 1 {
		batchSize = 1
	}

	return &InsertGenerator{
		table:       table,
		columnNames: names,
		batchSize:   batchSize,
		batch:       make([][]string, 0, batchSize),
	}
}

// AddRow adds a row with ID and formatted values to the generator.
// Returns an INSERT statement if the batch is full, otherwise returns empty string.
func (g *InsertGenerator) AddRow(id string, values []string) string {
	// Prepend ID to values
	row := make([]string, len(values)+1)
	row[0] = id
	copy(row[1:], values)

	g.batch = append(g.batch, row)

	if len(g.batch) >= g.batchSize {
		return g.flushBatch()
	}

	return ""
}

// Flush returns any remaining rows as an INSERT statement.
// Returns empty string if there are no pending rows.
func (g *InsertGenerator) Flush() string {
	if len(g.batch) == 0 {
		return ""
	}
	return g.flushBatch()
}

func (g *InsertGenerator) flushBatch() string {
	if len(g.batch) == 0 {
		return ""
	}

	var sb strings.Builder

	// INSERT INTO table (col1, col2, ...) VALUES
	sb.WriteString("INSERT INTO ")
	sb.WriteString(g.table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(g.columnNames, ", "))
	sb.WriteString(") VALUES ")

	// Generate value tuples
	for i, row := range g.batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		sb.WriteString(strings.Join(row, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(";")

	// Clear the batch
	g.batch = g.batch[:0]

	return sb.String()
}

// GenerateSingle generates a single INSERT statement for one row (no batching).
func GenerateSingle(table string, columnNames []string, values []string) string {
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columnNames, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(values, ", "))
	sb.WriteString(");")

	return sb.String()
}
