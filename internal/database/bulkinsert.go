package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

// InsertBatch performs a bulk copy of rows into the target table using mssql.CopyIn.
// Each batch runs inside its own transaction so a failure doesn't affect other batches.
func InsertBatch(ctx context.Context, db *sql.DB, schemaTable string, columns []string, rows [][]interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(schemaTable, mssql.BulkOptions{}, columns...))
	if err != nil {
		return fmt.Errorf("prepare copy: %w", err)
	}
	defer stmt.Close() //nolint:errcheck

	for i, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("exec row %d: %w", i, err)
		}
	}

	// Flush the bulk copy
	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("flush bulk copy: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
