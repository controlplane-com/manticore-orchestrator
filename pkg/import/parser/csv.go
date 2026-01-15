package parser

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

// CSVProcessor wraps a csv.Reader with streaming capabilities.
type CSVProcessor struct {
	reader      *csv.Reader
	lineNum     int
	autoID      bool     // true if auto-generating IDs
	nextID      int64    // next ID to assign (starts at 1)
	firstRecord []string // stores peeked first row
}

// NewCSVProcessor creates a new CSV processor from the given reader.
// If skipHeader is true, the first row will be read and discarded.
// The delimiter parameter specifies the field separator (e.g., ',' for CSV, '\t' for TSV).
// Auto-ID mode is enabled if the first data row's first column is not a valid integer.
func NewCSVProcessor(r io.Reader, skipHeader bool, delimiter rune) (*CSVProcessor, error) {
	reader := csv.NewReader(bufio.NewReader(r))
	reader.Comma = delimiter
	reader.FieldsPerRecord = -1 // Allow variable field counts (we validate later)
	reader.LazyQuotes = true    // Be lenient with quotes

	p := &CSVProcessor{
		reader:  reader,
		lineNum: 0,
	}

	if skipHeader {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("CSV file is empty")
			}
			return nil, fmt.Errorf("failed to skip header: %w", err)
		}
		p.lineNum = 1
	}

	// Peek at first data row to detect auto-ID mode
	firstRecord, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			// Empty file (or only header) - return processor, Next() will return EOF
			return p, nil
		}
		return nil, fmt.Errorf("failed to read first data row: %w", err)
	}

	// Check if first column is a valid integer (ID)
	if len(firstRecord) > 0 {
		if _, err := strconv.ParseInt(firstRecord[0], 10, 64); err != nil {
			// First column is not a valid ID, enable auto-ID mode
			p.autoID = true
			p.nextID = 1
		}
	}

	p.firstRecord = firstRecord
	return p, nil
}

// Next reads and returns the next record from the CSV.
// Returns io.EOF when there are no more records.
// In auto-ID mode, a generated ID is prepended to each record.
func (p *CSVProcessor) Next() ([]string, int, error) {
	var record []string

	// Return stored first record if available
	if p.firstRecord != nil {
		record = p.firstRecord
		p.firstRecord = nil
	} else {
		var err error
		record, err = p.reader.Read()
		if err != nil {
			return nil, p.lineNum, err
		}
	}

	p.lineNum++

	// In auto-ID mode, prepend generated ID to record
	if p.autoID {
		id := strconv.FormatInt(p.nextID, 10)
		p.nextID++
		record = append([]string{id}, record...)
	}

	return record, p.lineNum, nil
}

// LineNum returns the current line number (1-indexed).
func (p *CSVProcessor) LineNum() int {
	return p.lineNum
}
