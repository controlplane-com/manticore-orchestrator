package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cuppojoe/csv-to-manticore/pkg/config"
	"github.com/cuppojoe/csv-to-manticore/pkg/generator"
	"github.com/cuppojoe/csv-to-manticore/pkg/parser"
)

const version = "1.0.0"

var (
	csvFile     string
	configFile  string
	tableName   string
	skipHeader  bool
	batchSize   int
	showVersion bool
)

func init() {
	flag.StringVar(&csvFile, "file", "", "Path to CSV file (required, use - for stdin)")
	flag.StringVar(&csvFile, "f", "", "Path to CSV file (shorthand)")

	flag.StringVar(&configFile, "config", "", "Path to column mapping config JSON file (required)")
	flag.StringVar(&configFile, "c", "", "Path to config file (shorthand)")

	flag.StringVar(&tableName, "table", "", "Target table name (required)")
	flag.StringVar(&tableName, "t", "", "Table name (shorthand)")

	flag.BoolVar(&skipHeader, "skip-header", false, "Skip the first row of the CSV file")
	flag.BoolVar(&skipHeader, "s", false, "Skip header (shorthand)")

	flag.IntVar(&batchSize, "batch-size", 1, "Number of rows per INSERT statement")
	flag.IntVar(&batchSize, "b", 1, "Batch size (shorthand)")

	flag.BoolVar(&showVersion, "version", false, "Display version information")
	flag.BoolVar(&showVersion, "v", false, "Version (shorthand)")

	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "csv-to-manticore - Convert CSV files to Manticore INSERT statements\n\n")
		_, _ = fmt.Fprintf(os.Stderr, "Usage:\n  csv-to-manticore [flags]\n\nFlags:\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -f, --file string        Path to CSV file (required, use - for stdin)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -c, --config string      Path to column mapping config JSON file (required)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -t, --table string       Target table name (required)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -s, --skip-header        Skip the first row of the CSV file\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -b, --batch-size int     Number of rows per INSERT statement (default: 1)\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -v, --version            Display version information\n")
		_, _ = fmt.Fprintf(os.Stderr, "  -h, --help               Display help information\n")
		_, _ = fmt.Fprintf(os.Stderr, "\nExamples:\n")
		_, _ = fmt.Fprintf(os.Stderr, "  csv-to-manticore -f data.csv -c mapping.json -t products --skip-header\n")
		_, _ = fmt.Fprintf(os.Stderr, "  cat data.csv | csv-to-manticore -f - -c mapping.json -t products -s\n")
		_, _ = fmt.Fprintf(os.Stderr, "  csv-to-manticore -f large.csv -c mapping.json -t items -s -b 100\n")
	}
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("csv-to-manticore version %s\n", version)
		os.Exit(0)
	}

	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "csv-to-manticore: %v\n", err)
		if _, ok := err.(*parser.ParseError); ok {
			os.Exit(1)
		}
		os.Exit(2)
	}
}

func run() error {
	// Validate required flags
	if csvFile == "" {
		return fmt.Errorf("error: --file is required")
	}
	if configFile == "" {
		return fmt.Errorf("error: --config is required")
	}
	if tableName == "" {
		return fmt.Errorf("error: --table is required")
	}

	// Load configuration
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return err
	}

	// Determine delimiter based on file extension
	delimiter := ','
	if strings.HasSuffix(strings.ToLower(csvFile), ".tsv") {
		delimiter = '\t'
	}

	// Open CSV input
	var input io.Reader
	if csvFile == "-" {
		input = os.Stdin
	} else {
		f, err := os.Open(csvFile)
		if err != nil {
			return fmt.Errorf("opening CSV file: %w", err)
		}
		defer func(f *os.File) {
			_ = f.Close()
		}(f)
		input = f
	}

	// Process CSV
	return processCSV(input, cfg, tableName, skipHeader, batchSize, delimiter, os.Stdout)
}

func processCSV(input io.Reader, cfg *config.Config, table string, skipHeader bool, batchSize int, delimiter rune, output io.Writer) error {
	csvProcessor, err := parser.NewCSVProcessor(input, skipHeader, delimiter)
	if err != nil {
		return err
	}

	gen := generator.NewInsertGenerator(table, cfg.Columns, batchSize)
	var parseErrors []*parser.ParseError
	successCount := 0

	for {
		record, lineNum, err := csvProcessor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}

		// Convert values based on column types (first column is ID)
		id, values, err := parser.ConvertRow(record, cfg.Columns, lineNum)
		if err != nil {
			// Log error immediately to stderr
			if parseErr, ok := err.(*parser.ParseError); ok {
				_, _ = fmt.Fprintf(os.Stderr, "warning: %v\n", parseErr)
				parseErrors = append(parseErrors, parseErr)
			} else {
				_, _ = fmt.Fprintf(os.Stderr, "warning: line %d: %v\n", lineNum, err)
			}
			continue // Skip this row
		}

		// Generate INSERT statement
		if stmt := gen.AddRow(id, values); stmt != "" {
			if _, err := fmt.Fprintln(output, stmt); err != nil {
				return fmt.Errorf("write error (downstream process may have died): %w", err)
			}
		}
		successCount++
	}

	// Flush any remaining batch
	if stmt := gen.Flush(); stmt != "" {
		if _, err := fmt.Fprintln(output, stmt); err != nil {
			return fmt.Errorf("write error (downstream process may have died): %w", err)
		}
	}

	// Print summary if there were errors
	if len(parseErrors) > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "\n--- Parse Error Summary ---\n")
		_, _ = fmt.Fprintf(os.Stderr, "Rows processed successfully: %d\n", successCount)
		_, _ = fmt.Fprintf(os.Stderr, "Rows skipped due to errors: %d\n", len(parseErrors))
		_, _ = fmt.Fprintf(os.Stderr, "\nErrors:\n")
		for _, e := range parseErrors {
			_, _ = fmt.Fprintf(os.Stderr, "  %v\n", e)
		}
	}

	return nil
}
