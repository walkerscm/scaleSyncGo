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

	// 4. Confirm
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

	// 5. Run import using shared helper
	totalInserted, err := importFile(ctx, db, selectedCSV, selectedTable, batchSize, workers, os.Stdout)
	if err != nil {
		return err
	}
	_ = totalInserted

	return nil
}
