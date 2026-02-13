package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/walkerscm/scaleSyncGo/internal/config"
	"github.com/walkerscm/scaleSyncGo/internal/csvutil"
	"github.com/walkerscm/scaleSyncGo/internal/database"
	"github.com/walkerscm/scaleSyncGo/internal/worker"
)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import a CSV file into an Azure SQL table",
	Long:  `Interactively select a CSV file and target table, then bulk-insert rows using concurrent workers.`,
	RunE:  runImport,
}

func init() {
	importCmd.Flags().String("env", ".env", "path to .env file")
	importCmd.Flags().Int("batch-size", 1000, "rows per batch")
	importCmd.Flags().Int("workers", 4, "parallel worker count")
	importCmd.Flags().String("file", "", "path to CSV file (skips interactive selection)")
	importCmd.Flags().String("table", "", "target table as schema.name (skips interactive selection)")
	importCmd.Flags().BoolP("yes", "y", false, "skip confirmation prompt")
	rootCmd.AddCommand(importCmd)
}

func runImport(cmd *cobra.Command, args []string) error {
	envPath, _ := cmd.Flags().GetString("env")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	workers, _ := cmd.Flags().GetInt("workers")
	filePath, _ := cmd.Flags().GetString("file")
	tableName, _ := cmd.Flags().GetString("table")
	autoConfirm, _ := cmd.Flags().GetBool("yes")

	// 1. Select CSV file
	var selectedCSV string
	if filePath != "" {
		// Non-interactive: use provided path
		if _, err := os.Stat(filePath); err != nil {
			return fmt.Errorf("csv file not found: %s", filePath)
		}
		selectedCSV = filePath
	} else {
		// Interactive: scan and prompt
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("getting cwd: %w", err)
		}

		csvFiles, err := csvutil.ScanDirectory(cwd)
		if err != nil {
			return fmt.Errorf("scanning for CSV files: %w", err)
		}
		if len(csvFiles) == 0 {
			return fmt.Errorf("no .csv files found in %s", cwd)
		}

		names := make([]string, len(csvFiles))
		for i, f := range csvFiles {
			names[i] = filepath.Base(f)
		}

		csvPrompt := promptui.Select{
			Label: "Select CSV file",
			Items: names,
			Size:  15,
		}
		csvIdx, _, err := csvPrompt.Run()
		if err != nil {
			return fmt.Errorf("csv selection: %w", err)
		}
		selectedCSV = csvFiles[csvIdx]
	}
	fmt.Printf("Selected: %s\n", filepath.Base(selectedCSV))

	// 2. Load database config and connect
	dbCfg, err := config.LoadDatabaseConfig(envPath)
	if err != nil {
		return fmt.Errorf("loading database config: %w", err)
	}

	fmt.Printf("Connecting to %s/%s...\n", dbCfg.Server, dbCfg.Database)
	db, err := database.NewConnection(dbCfg.ConnectionString())
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer db.Close()
	fmt.Println("Connected.")

	// 3. Select target table
	ctx := context.Background()
	var selectedTable string
	if tableName != "" {
		selectedTable = tableName
	} else {
		tables, err := database.ListTables(ctx, db)
		if err != nil {
			return fmt.Errorf("listing tables: %w", err)
		}
		if len(tables) == 0 {
			return fmt.Errorf("no tables found in database")
		}

		tablePrompt := promptui.Select{
			Label:             "Select target table",
			Items:             tables,
			Size:              15,
			StartInSearchMode: true,
			Searcher: func(input string, index int) bool {
				return strings.Contains(strings.ToLower(tables[index]), strings.ToLower(input))
			},
		}
		_, selectedTable, err = tablePrompt.Run()
		if err != nil {
			return fmt.Errorf("table selection: %w", err)
		}
	}
	fmt.Printf("Target table: %s\n", selectedTable)

	// 4. Get table schema
	tableCols, err := database.GetTableColumns(ctx, db, selectedTable)
	if err != nil {
		return fmt.Errorf("getting table columns: %w", err)
	}
	fmt.Printf("Table has %d columns\n", len(tableCols))

	// 5. Open CSV, read headers, map columns
	reader, err := csvutil.NewReader(selectedCSV)
	if err != nil {
		return fmt.Errorf("opening CSV: %w", err)
	}
	defer reader.Close()

	headers := reader.Headers()
	fmt.Printf("CSV has %d columns\n", len(headers))

	mapResult, err := database.MapColumns(headers, tableCols)
	if err != nil {
		return fmt.Errorf("column mapping: %w", err)
	}

	fmt.Printf("Matched %d columns\n", len(mapResult.Mapped))
	if len(mapResult.Skipped) > 0 {
		fmt.Printf("Skipped %d CSV columns (no table match): %s\n",
			len(mapResult.Skipped), strings.Join(mapResult.Skipped, ", "))
	}

	// 6. Confirm
	if !autoConfirm {
		confirmPrompt := promptui.Prompt{
			Label:     fmt.Sprintf("Import %s into %s", filepath.Base(selectedCSV), selectedTable),
			IsConfirm: true,
		}
		if _, err := confirmPrompt.Run(); err != nil {
			fmt.Println("Import cancelled.")
			return nil
		}
	}

	// Build column names list for bulk insert
	dbColumns := make([]string, len(mapResult.Mapped))
	for i, m := range mapResult.Mapped {
		dbColumns[i] = m.DBColumn.Name
	}

	// 7. Read CSV in batches and submit to worker pool
	pool := worker.NewPool(db, selectedTable, dbColumns, mapResult.Mapped, workers)
	pool.Start(ctx)

	// Count total rows for progress bar (quick scan)
	totalRows := countCSVRows(selectedCSV)
	bar := progressbar.NewOptions(totalRows,
		progressbar.OptionSetDescription("Importing"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionThrottle(200*time.Millisecond),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("rows"),
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
	)

	// Feed batches from a goroutine so we can drain results concurrently
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

	// 9. Collect results
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

	// 10. Print summary
	elapsed := time.Since(start)
	rowsPerSec := float64(totalInserted) / elapsed.Seconds()

	fmt.Println("\n--- Import Summary ---")
	fmt.Printf("Total rows inserted: %d\n", totalInserted)
	fmt.Printf("Duration:            %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput:          %.0f rows/sec\n", rowsPerSec)
	fmt.Printf("Errors:              %d\n", errorCount)

	if len(errors) > 0 {
		fmt.Println("\nFirst errors:")
		for _, e := range errors {
			fmt.Printf("  - %s\n", e)
		}
	}

	return nil
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
