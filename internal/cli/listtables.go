package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/walkerscm/scaleSyncGo/internal/config"
	"github.com/walkerscm/scaleSyncGo/internal/database"
)

const reportsDir = "reports"

var listTablesCmd = &cobra.Command{
	Use:   "list-tables",
	Short: "List all tables in the target database",
	RunE:  runListTables,
}

func init() {
	listTablesCmd.Flags().String("env", ".env", "path to .env file")
	listTablesCmd.Flags().Bool("counts", false, "include row counts for each table")
	listTablesCmd.Flags().String("markdown", "", "write output to a markdown file (default: reports/tables-report-<timestamp>.md)")
	listTablesCmd.Flags().Bool("md", false, "shorthand: write markdown report to reports/ with auto-generated filename")
	rootCmd.AddCommand(listTablesCmd)
}

func runListTables(cmd *cobra.Command, args []string) error {
	envPath, _ := cmd.Flags().GetString("env")
	showCounts, _ := cmd.Flags().GetBool("counts")
	mdPath, _ := cmd.Flags().GetString("markdown")
	mdShort, _ := cmd.Flags().GetBool("md")

	// --md is a shorthand that auto-generates a timestamped path in reports/
	if mdShort && mdPath == "" {
		mdPath = filepath.Join(reportsDir,
			fmt.Sprintf("tables-report-%s.md", time.Now().Format("20060102-150405")))
	}

	dbCfg, err := config.LoadDatabaseConfig(envPath)
	if err != nil {
		return err
	}

	db, err := database.NewConnection(dbCfg.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()

	// Always fetch counts when writing markdown
	if mdPath != "" {
		showCounts = true
	}

	if showCounts {
		results, err := database.ListTablesWithCounts(ctx, db)
		if err != nil {
			return err
		}

		// Find longest name for alignment
		maxLen := 0
		for _, r := range results {
			if len(r.Name) > maxLen {
				maxLen = len(r.Name)
			}
		}

		// Print to stdout
		for _, r := range results {
			fmt.Printf("%-*s  %d\n", maxLen, r.Name, r.RowCount)
		}

		// Write markdown if requested
		if mdPath != "" {
			if err := writeMarkdown(mdPath, dbCfg, results); err != nil {
				return err
			}
			fmt.Printf("\nMarkdown written to %s\n", mdPath)
		}
		return nil
	}

	tables, err := database.ListTables(ctx, db)
	if err != nil {
		return err
	}
	for _, t := range tables {
		fmt.Println(t)
	}
	return nil
}

func writeMarkdown(path string, dbCfg *config.DatabaseConfig, results []database.TableRowCount) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("creating directory %s: %w", dir, err)
		}
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating markdown file: %w", err)
	}
	defer f.Close()

	var totalRows int64
	for _, r := range results {
		totalRows += r.RowCount
	}

	fmt.Fprintf(f, "# Database Tables Report\n\n")
	fmt.Fprintf(f, "**Server:** `%s`\n", dbCfg.Server)
	fmt.Fprintf(f, "**Database:** `%s`\n", dbCfg.Database)
	fmt.Fprintf(f, "**Generated:** %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(f, "**Total Tables:** %d\n", len(results))
	fmt.Fprintf(f, "**Total Rows:** %d\n\n", totalRows)

	fmt.Fprintf(f, "| # | Table | Row Count |\n")
	fmt.Fprintf(f, "|---|-------|----------:|\n")
	for i, r := range results {
		fmt.Fprintf(f, "| %d | `%s` | %d |\n", i+1, r.Name, r.RowCount)
	}

	return nil
}
