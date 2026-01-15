package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/controlplane-com/manticore-orchestrator/pkg/import/config"
	"github.com/controlplane-com/manticore-orchestrator/pkg/import/generator"
)

// ParseError represents an error that occurred while parsing a specific column.
type ParseError struct {
	Line   int
	Column string
	Value  string
	Reason string
}

func (e *ParseError) Error() string {
	if e.Value != "" {
		return fmt.Sprintf("line %d: column %q: %s (value: %q)", e.Line, e.Column, e.Reason, e.Value)
	}
	return fmt.Sprintf("line %d: column %q: %s", e.Line, e.Column, e.Reason)
}

// ConvertRow converts a CSV record to SQL-formatted values based on column types.
// The first column is treated as the document ID (bigint).
// Returns (id, values, error) where values are for columns 2-N.
func ConvertRow(record []string, columns []config.Column, lineNum int) (string, []string, error) {
	// First column is always the ID
	if len(record) < 1 {
		return "", nil, &ParseError{
			Line:   lineNum,
			Column: "",
			Reason: "empty record",
		}
	}

	// Validate remaining columns match config
	if len(record)-1 != len(columns) {
		return "", nil, &ParseError{
			Line:   lineNum,
			Column: "",
			Reason: fmt.Sprintf("expected %d data columns (plus ID), got %d total columns", len(columns), len(record)),
		}
	}

	// Parse ID as bigint
	id, err := formatBigint(record[0])
	if err != nil {
		return "", nil, &ParseError{
			Line:   lineNum,
			Column: "id",
			Value:  record[0],
			Reason: fmt.Sprintf("invalid ID: %v", err),
		}
	}

	// Convert remaining columns
	values := make([]string, len(columns))
	for i, cell := range record[1:] {
		col := columns[i]
		formatted, err := formatValue(cell, col.Type)
		if err != nil {
			return "", nil, &ParseError{
				Line:   lineNum,
				Column: col.Name,
				Value:  cell,
				Reason: err.Error(),
			}
		}
		values[i] = formatted
	}

	return id, values, nil
}

// formatValue converts a string cell value to its SQL representation based on column type.
func formatValue(value string, colType config.ColumnType) (string, error) {
	value = strings.TrimSpace(value)

	switch colType {
	case config.TypeField, config.TypeFieldString, config.TypeAttrString:
		return generator.QuoteString(value), nil

	case config.TypeAttrUint:
		return formatUint(value)

	case config.TypeAttrBigint:
		return formatBigint(value)

	case config.TypeAttrFloat:
		return formatFloat(value)

	case config.TypeAttrBool:
		return formatBool(value)

	case config.TypeAttrTimestamp:
		return formatTimestamp(value)

	case config.TypeAttrMulti:
		return formatMVA(value, 32)

	case config.TypeAttrMulti64:
		return formatMVA(value, 64)

	case config.TypeAttrJSON:
		return formatJSON(value)

	default:
		return "", fmt.Errorf("unknown column type: %s", colType)
	}
}

func formatUint(value string) (string, error) {
	if value == "" {
		return "0", nil
	}
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return "", fmt.Errorf("invalid unsigned integer: %w", err)
	}
	return strconv.FormatUint(n, 10), nil
}

func formatBigint(value string) (string, error) {
	if value == "" {
		return "0", nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid integer: %w", err)
	}
	return strconv.FormatInt(n, 10), nil
}

func formatFloat(value string) (string, error) {
	if value == "" {
		return "0", nil
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", fmt.Errorf("invalid float: %w", err)
	}
	return strconv.FormatFloat(f, 'f', -1, 64), nil
}

func formatBool(value string) (string, error) {
	value = strings.ToLower(value)
	switch value {
	case "1", "true", "yes", "on", "t", "y":
		return "1", nil
	case "0", "false", "no", "off", "f", "n", "":
		return "0", nil
	default:
		return "", fmt.Errorf("invalid boolean value: %q", value)
	}
}

// timestampFormats lists common date/time formats to try when parsing timestamps.
var timestampFormats = []string{
	time.RFC3339,
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02",
	"01/02/2006",
	"01/02/2006 15:04:05",
	"02-Jan-2006",
	"02-Jan-2006 15:04:05",
}

func formatTimestamp(value string) (string, error) {
	if value == "" {
		return "0", nil
	}

	// Try parsing as Unix timestamp first
	if ts, err := strconv.ParseInt(value, 10, 64); err == nil {
		return strconv.FormatInt(ts, 10), nil
	}

	// Try known date formats
	for _, format := range timestampFormats {
		if t, err := time.Parse(format, value); err == nil {
			return strconv.FormatInt(t.Unix(), 10), nil
		}
	}

	return "", fmt.Errorf("unrecognized timestamp format: %q", value)
}

// formatMVA formats a multi-value attribute as a tuple (1,2,3).
// bits should be 32 or 64 to indicate the integer size.
func formatMVA(value string, bits int) (string, error) {
	if value == "" {
		return "()", nil
	}

	nums, err := parseMVA(value, bits)
	if err != nil {
		return "", err
	}

	if len(nums) == 0 {
		return "()", nil
	}

	parts := make([]string, len(nums))
	for i, n := range nums {
		parts[i] = strconv.FormatInt(n, 10)
	}
	return "(" + strings.Join(parts, ",") + ")", nil
}

func parseMVA(value string, bits int) ([]int64, error) {
	value = strings.TrimSpace(value)

	// Handle JSON array format
	if strings.HasPrefix(value, "[") {
		var nums []int64
		if err := json.Unmarshal([]byte(value), &nums); err != nil {
			return nil, fmt.Errorf("invalid JSON array: %w", err)
		}
		return nums, nil
	}

	// Handle comma or space separated values
	sep := ","
	if !strings.Contains(value, ",") && strings.Contains(value, " ") {
		sep = " "
	}

	var nums []int64
	for _, part := range strings.Split(value, sep) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var n int64
		var err error
		if bits == 32 {
			var u uint64
			u, err = strconv.ParseUint(part, 10, 32)
			n = int64(u)
		} else {
			n, err = strconv.ParseInt(part, 10, 64)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", part, err)
		}
		nums = append(nums, n)
	}

	return nums, nil
}

func formatJSON(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "'{}'", nil
	}

	// Validate it's proper JSON
	var js json.RawMessage
	if err := json.Unmarshal([]byte(value), &js); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	// Return quoted JSON string
	return generator.QuoteString(string(js)), nil
}

// ConvertRowJSON converts a CSV record to JSON-native Go types.
// The first column is treated as the document ID (int64).
// Returns (id, values, error) where values are native Go types for JSON serialization.
func ConvertRowJSON(record []string, columns []config.Column, lineNum int) (int64, []interface{}, error) {
	// First column is always the ID
	if len(record) < 1 {
		return 0, nil, &ParseError{
			Line:   lineNum,
			Column: "",
			Reason: "empty record",
		}
	}

	// Validate remaining columns match config
	if len(record)-1 != len(columns) {
		return 0, nil, &ParseError{
			Line:   lineNum,
			Column: "",
			Reason: fmt.Sprintf("expected %d data columns (plus ID), got %d total columns", len(columns), len(record)),
		}
	}

	// Parse ID as int64
	id, err := parseJSONBigint(record[0])
	if err != nil {
		return 0, nil, &ParseError{
			Line:   lineNum,
			Column: "id",
			Value:  record[0],
			Reason: fmt.Sprintf("invalid ID: %v", err),
		}
	}

	// Convert remaining columns to JSON-native types
	values := make([]interface{}, len(columns))
	for i, cell := range record[1:] {
		col := columns[i]
		val, err := convertToJSON(cell, col.Type)
		if err != nil {
			return 0, nil, &ParseError{
				Line:   lineNum,
				Column: col.Name,
				Value:  cell,
				Reason: err.Error(),
			}
		}
		values[i] = val
	}

	return id, values, nil
}

// convertToJSON converts a string value to a JSON-native Go type.
func convertToJSON(value string, colType config.ColumnType) (interface{}, error) {
	value = strings.TrimSpace(value)

	switch colType {
	case config.TypeField, config.TypeFieldString, config.TypeAttrString:
		// Return string as-is (will be JSON string)
		return value, nil

	case config.TypeAttrUint:
		return parseJSONUint(value)

	case config.TypeAttrBigint:
		return parseJSONBigint(value)

	case config.TypeAttrFloat:
		return parseJSONFloat(value)

	case config.TypeAttrBool:
		return parseJSONBool(value)

	case config.TypeAttrTimestamp:
		return parseJSONTimestamp(value)

	case config.TypeAttrMulti:
		return parseMVA(value, 32)

	case config.TypeAttrMulti64:
		return parseMVA(value, 64)

	case config.TypeAttrJSON:
		return parseJSONObject(value)

	default:
		return value, nil
	}
}

func parseJSONUint(value string) (interface{}, error) {
	if value == "" {
		return uint64(0), nil
	}
	return strconv.ParseUint(value, 10, 32)
}

func parseJSONBigint(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func parseJSONFloat(value string) (interface{}, error) {
	if value == "" {
		return float64(0), nil
	}
	return strconv.ParseFloat(value, 64)
}

func parseJSONBool(value string) (interface{}, error) {
	value = strings.ToLower(value)
	switch value {
	case "1", "true", "yes", "on", "t", "y":
		return true, nil
	case "0", "false", "no", "off", "f", "n", "":
		return false, nil
	default:
		return nil, fmt.Errorf("invalid boolean value: %q", value)
	}
}

func parseJSONTimestamp(value string) (interface{}, error) {
	if value == "" {
		return int64(0), nil
	}

	// Try parsing as Unix timestamp first
	if ts, err := strconv.ParseInt(value, 10, 64); err == nil {
		return ts, nil
	}

	// Try known date formats
	for _, format := range timestampFormats {
		if t, err := time.Parse(format, value); err == nil {
			return t.Unix(), nil
		}
	}

	return nil, fmt.Errorf("unrecognized timestamp format: %q", value)
}

func parseJSONObject(value string) (interface{}, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return map[string]interface{}{}, nil
	}

	// Parse and return native JSON object/array
	var obj interface{}
	if err := json.Unmarshal([]byte(value), &obj); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return obj, nil
}
