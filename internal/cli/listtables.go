package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/walkerscm/scaleSyncGo/internal/config"
	"github.com/walkerscm/scaleSyncGo/internal/database"
)

var listTablesCmd = &cobra.Command{
	Use:   "list-tables",
	Short: "List all tables in the target database",
	RunE:  runListTables,
}

func init() {
	listTablesCmd.Flags().String("env", ".env", "path to .env file")
	rootCmd.AddCommand(listTablesCmd)
}

func runListTables(cmd *cobra.Command, args []string) error {
	envPath, _ := cmd.Flags().GetString("env")

	dbCfg, err := config.LoadDatabaseConfig(envPath)
	if err != nil {
		return err
	}

	db, err := database.NewConnection(dbCfg.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	tables, err := database.ListTables(context.Background(), db)
	if err != nil {
		return err
	}

	for _, t := range tables {
		fmt.Println(t)
	}
	return nil
}
