package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

// InsertBatch performs an upsert of rows into the target table using the
// temp-table + MERGE pattern. If pkColumns is empty, it falls back to a
// straight bulk copy (insert-only).
func InsertBatch(ctx context.Context, db *sql.DB, schemaTable string, columns []string, pkColumns []string, hasIdentity bool, rows [][]interface{}) error {
	if len(pkColumns) == 0 {
		return insertBatchDirect(ctx, db, schemaTable, columns, rows)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// 1. Create temp table matching target schema (no constraints).
	createTemp := fmt.Sprintf("SELECT TOP(0) * INTO #temp FROM %s", schemaTable)
	if _, err := tx.ExecContext(ctx, createTemp); err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	// 2. Bulk copy rows into #temp.
	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn("#temp", mssql.BulkOptions{}, columns...))
	if err != nil {
		return fmt.Errorf("prepare copy: %w", err)
	}
	defer stmt.Close() //nolint:errcheck

	for i, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("exec row %d: %w", i, err)
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("flush bulk copy: %w", err)
	}

	// 3. MERGE into target.
	if hasIdentity {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET IDENTITY_INSERT %s ON", schemaTable)); err != nil {
			return fmt.Errorf("identity insert on: %w", err)
		}
	}
	mergeSQL := buildMergeSQL(schemaTable, columns, pkColumns)
	if _, err := tx.ExecContext(ctx, mergeSQL); err != nil {
		return fmt.Errorf("merge: %w", err)
	}
	if hasIdentity {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET IDENTITY_INSERT %s OFF", schemaTable)); err != nil {
			return fmt.Errorf("identity insert off: %w", err)
		}
	}

	// 4. Drop temp table.
	if _, err := tx.ExecContext(ctx, "DROP TABLE #temp"); err != nil {
		return fmt.Errorf("drop temp: %w", err)
	}

	// 5. Commit.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// insertBatchDirect is the original straight bulk-copy path for tables without a PK.
func insertBatchDirect(ctx context.Context, db *sql.DB, schemaTable string, columns []string, rows [][]interface{}) error {
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

	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("flush bulk copy: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// buildMergeSQL constructs a MERGE statement that upserts from #temp into the target table.
func buildMergeSQL(schemaTable string, columns, pkColumns []string) string {
	// Build ON clause from PK columns.
	onParts := make([]string, len(pkColumns))
	for i, pk := range pkColumns {
		onParts[i] = fmt.Sprintf("target.[%s] = source.[%s]", pk, pk)
	}
	onClause := strings.Join(onParts, " AND ")

	// Build UPDATE SET clause from non-PK columns.
	pkSet := make(map[string]bool, len(pkColumns))
	for _, pk := range pkColumns {
		pkSet[strings.ToUpper(pk)] = true
	}
	var setParts []string
	for _, col := range columns {
		if !pkSet[strings.ToUpper(col)] {
			setParts = append(setParts, fmt.Sprintf("target.[%s] = source.[%s]", col, col))
		}
	}

	// Build INSERT column and value lists.
	colList := make([]string, len(columns))
	valList := make([]string, len(columns))
	for i, col := range columns {
		colList[i] = fmt.Sprintf("[%s]", col)
		valList[i] = fmt.Sprintf("source.[%s]", col)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "MERGE %s AS target ", schemaTable)
	fmt.Fprintf(&b, "USING #temp AS source ")
	fmt.Fprintf(&b, "ON (%s) ", onClause)
	if len(setParts) > 0 {
		fmt.Fprintf(&b, "WHEN MATCHED THEN UPDATE SET %s ", strings.Join(setParts, ", "))
	}
	fmt.Fprintf(&b, "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(colList, ", "), strings.Join(valList, ", "))

	return b.String()
}
