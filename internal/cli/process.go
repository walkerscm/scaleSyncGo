package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/walkerscm/scaleSyncGo/internal/config"
	"github.com/walkerscm/scaleSyncGo/internal/csvutil"
	"github.com/walkerscm/scaleSyncGo/internal/database"
)

const (
	inputDir     = "csv_input"
	processedDir = "csv_processed"
)

var processCmd = &cobra.Command{
	Use:   "process",
	Short: "Batch-process all CSV files in csv_input/",
	Long:  `Scan csv_input/ for CSV files, match each to a database table by filename, import them sequentially, and move completed files to csv_processed/.`,
	RunE:  runProcess,
}

func init() {
	processCmd.Flags().String("env", ".env", "path to .env file")
	processCmd.Flags().Int("batch-size", 1000, "rows per batch")
	processCmd.Flags().Int("workers", 4, "parallel worker count")
	processCmd.Flags().BoolP("yes", "y", false, "skip confirmation prompt")
	rootCmd.AddCommand(processCmd)
}

// csvMatch pairs a CSV file path with its resolved database table name.
type csvMatch struct {
	Path        string
	TableName   string // e.g. "dbo.SHIPPING_CONTAINER"
	BaseName    string // original filename
	ExtractedID string // table portion extracted from filename
}

func runProcess(cmd *cobra.Command, args []string) error {
	envPath, _ := cmd.Flags().GetString("env")
	target, _ := cmd.Flags().GetString("target")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	workers, _ := cmd.Flags().GetInt("workers")
	autoConfirm, _ := cmd.Flags().GetBool("yes")

	// 1. Scan csv_input/
	csvFiles, err := csvutil.ScanDirectory(inputDir)
	if err != nil {
		return fmt.Errorf("scanning %s: %w", inputDir, err)
	}
	if len(csvFiles) == 0 {
		fmt.Printf("No CSV files found in %s/\n", inputDir)
		return nil
	}
	fmt.Printf("Found %d CSV file(s) in %s/\n", len(csvFiles), inputDir)

	// 2. Connect to database
	dbCfg, err := config.LoadDatabaseConfig(envPath, target)
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

	ctx := context.Background()
	tables, err := database.ListTables(ctx, db)
	if err != nil {
		return fmt.Errorf("listing tables: %w", err)
	}

	// Build lowercase lookup: "dbo.shipping_container" → "dbo.SHIPPING_CONTAINER"
	tableLookup := make(map[string]string, len(tables))
	for _, t := range tables {
		tableLookup[strings.ToLower(t)] = t
	}

	// 3. Match each CSV file to a table
	var matched []csvMatch
	var unmatched []string

	for _, csvPath := range csvFiles {
		baseName := filepath.Base(csvPath)
		tablePart := extractTableName(baseName)
		if tablePart == "" {
			unmatched = append(unmatched, baseName)
			continue
		}

		// Look up as dbo.<TABLE_NAME>
		key := strings.ToLower("dbo." + tablePart)
		if fullName, ok := tableLookup[key]; ok {
			matched = append(matched, csvMatch{
				Path:        csvPath,
				TableName:   fullName,
				BaseName:    baseName,
				ExtractedID: tablePart,
			})
		} else {
			unmatched = append(unmatched, baseName+" (no table: dbo."+tablePart+")")
		}
	}

	// 4. Print summary
	fmt.Println()
	fmt.Printf("Matched:   %d file(s)\n", len(matched))
	for _, m := range matched {
		fmt.Printf("  %s → %s\n", m.BaseName, m.TableName)
	}
	if len(unmatched) > 0 {
		fmt.Printf("Unmatched: %d file(s)\n", len(unmatched))
		for _, u := range unmatched {
			fmt.Printf("  %s\n", u)
		}
	}
	fmt.Println()

	if len(matched) == 0 {
		fmt.Println("No files matched any database table. Nothing to do.")
		return nil
	}

	// 5. Confirm
	if !autoConfirm {
		confirmPrompt := promptui.Prompt{
			Label:     fmt.Sprintf("Process %d file(s)", len(matched)),
			IsConfirm: true,
		}
		if _, err := confirmPrompt.Run(); err != nil {
			fmt.Println("Processing cancelled.")
			return nil
		}
	}

	// Ensure csv_processed/ exists
	if err := os.MkdirAll(processedDir, 0o755); err != nil {
		return fmt.Errorf("creating %s directory: %w", processedDir, err)
	}

	// 6. Process each matched file sequentially
	var succeeded, failed, totalRows int

	for i, m := range matched {
		fmt.Printf("\n[%d/%d] Processing %s → %s...\n", i+1, len(matched), m.BaseName, m.TableName)

		rows, err := importFile(ctx, db, m.Path, m.TableName, batchSize, workers, os.Stdout)
		if err != nil {
			fmt.Printf("ERROR: %s: %v\n", m.BaseName, err)
			failed++
			continue
		}

		totalRows += rows
		succeeded++

		// Move file to csv_processed/
		destPath := filepath.Join(processedDir, m.BaseName)
		if err := os.Rename(m.Path, destPath); err != nil {
			fmt.Printf("WARNING: imported OK but failed to move %s: %v\n", m.BaseName, err)
		} else {
			fmt.Printf("Moved %s → %s/\n", m.BaseName, processedDir)
		}
	}

	// 7. Final summary
	fmt.Println("\n=== Process Summary ===")
	fmt.Printf("Files processed: %d\n", len(matched))
	fmt.Printf("Succeeded:       %d\n", succeeded)
	fmt.Printf("Failed:          %d\n", failed)
	fmt.Printf("Total rows:      %d\n", totalRows)

	return nil
}

// extractTableName gets the table name from a filename like "TABLE_NAME_inserts_20260211_170255.csv".
// It returns everything before "_inserts_", or empty string if the pattern is not found.
func extractTableName(filename string) string {
	// Remove extension
	name := strings.TrimSuffix(filename, filepath.Ext(filename))

	// Find "_inserts_" (case-insensitive)
	lower := strings.ToLower(name)
	idx := strings.Index(lower, "_inserts_")
	if idx < 0 {
		return ""
	}
	return name[:idx]
}
