package cli

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/walkerscm/scaleSyncGo/internal/csvutil"
	"github.com/walkerscm/scaleSyncGo/internal/database"
	"github.com/walkerscm/scaleSyncGo/internal/worker"
)

// importFile performs the core CSV-to-database import: reads columns, maps headers,
// runs worker pool, and returns total rows inserted. Progress is written to w.
func importFile(ctx context.Context, db *sql.DB, csvPath, schemaTable string, batchSize, workers int, w io.Writer) (int, error) {
	// Get table schema
	tableCols, err := database.GetTableColumns(ctx, db, schemaTable)
	if err != nil {
		return 0, fmt.Errorf("getting table columns: %w", err)
	}
	fmt.Fprintf(w, "Table has %d columns\n", len(tableCols))

	// Open CSV, read headers
	reader, err := csvutil.NewReader(csvPath)
	if err != nil {
		return 0, fmt.Errorf("opening CSV: %w", err)
	}
	defer reader.Close()

	headers := reader.Headers()
	fmt.Fprintf(w, "CSV has %d columns\n", len(headers))

	// Map columns
	mapResult, err := database.MapColumns(headers, tableCols)
	if err != nil {
		return 0, fmt.Errorf("column mapping: %w", err)
	}

	fmt.Fprintf(w, "Matched %d columns\n", len(mapResult.Mapped))
	if len(mapResult.Skipped) > 0 {
		fmt.Fprintf(w, "Skipped %d CSV columns (no table match): %s\n",
			len(mapResult.Skipped), strings.Join(mapResult.Skipped, ", "))
	}

	// Build column names list
	dbColumns := make([]string, len(mapResult.Mapped))
	for i, m := range mapResult.Mapped {
		dbColumns[i] = m.DBColumn.Name
	}

	// Start worker pool
	pool := worker.NewPool(db, schemaTable, dbColumns, mapResult.Mapped, workers)
	pool.Start(ctx)

	// Progress bar
	totalRows := countCSVRows(csvPath)
	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionSetDescription("Importing"),
		progressbar.OptionSetWriter(w),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionThrottle(200*time.Millisecond),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("rows"),
		progressbar.OptionOnCompletion(func() { fmt.Fprintln(w) }),
	)

	// Feed batches
	go func() {
		batchNum := 0
		for {
			rows, err := reader.ReadBatch(batchSize)
			if len(rows) > 0 {
				pool.Submit(worker.Job{BatchNum: batchNum, Rows: rows})
				batchNum++
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCSV read error: %v\n", err)
				break
			}
		}
		pool.Done()
	}()

	// Collect results
	start := time.Now()
	var totalInserted int
	var errorCount int
	var errors []string

	for result := range pool.Results() {
		if result.Err != nil {
			errorCount++
			if len(errors) < 10 {
				errors = append(errors, result.Err.Error())
			}
		} else {
			totalInserted += result.RowCount
		}
		bar.Add(result.RowCount) //nolint:errcheck
	}
	bar.Finish() //nolint:errcheck

	// Summary
	elapsed := time.Since(start)
	rowsPerSec := float64(totalInserted) / elapsed.Seconds()

	fmt.Fprintf(w, "\n--- Import Summary ---\n")
	fmt.Fprintf(w, "File:                %s\n", filepath.Base(csvPath))
	fmt.Fprintf(w, "Table:               %s\n", schemaTable)
	fmt.Fprintf(w, "Total rows inserted: %d\n", totalInserted)
	fmt.Fprintf(w, "Duration:            %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(w, "Throughput:          %.0f rows/sec\n", rowsPerSec)
	fmt.Fprintf(w, "Errors:              %d\n", errorCount)

	if len(errors) > 0 {
		fmt.Fprintln(w, "\nFirst errors:")
		for _, e := range errors {
			fmt.Fprintf(w, "  - %s\n", e)
		}
		return totalInserted, fmt.Errorf("%d batch errors during import", errorCount)
	}

	return totalInserted, nil
}

// countCSVRows does a quick line count of the file (minus the header).
func countCSVRows(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()

	buf := make([]byte, 64*1024)
	count := 0
	for {
		n, err := f.Read(buf)
		for _, b := range buf[:n] {
			if b == '\n' {
				count++
			}
		}
		if err != nil {
			break
		}
	}
	if count > 0 {
		count--
	}
	return count
}
