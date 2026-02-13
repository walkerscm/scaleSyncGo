package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TableColumn describes a column in a database table.
type TableColumn struct {
	Name       string
	DataType   string
	IsNullable bool
	OrdinalPos int
}

// ListTables returns all user table names from the connected database.
func ListTables(ctx context.Context, db *sql.DB) ([]string, error) {
	query := `SELECT s.name + '.' + t.name
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		ORDER BY s.name, t.name`

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying sys.tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// TableRowCount holds a table name and its row count.
type TableRowCount struct {
	Name     string
	RowCount int64
}

// ListTablesWithCounts returns all user tables with their row counts using
// sys.dm_db_partition_stats (heap/clustered index only) for fast results.
func ListTablesWithCounts(ctx context.Context, db *sql.DB) ([]TableRowCount, error) {
	query := `SELECT s.name + '.' + t.name, SUM(p.row_count)
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		JOIN sys.dm_db_partition_stats p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
		GROUP BY s.name, t.name
		ORDER BY s.name, t.name`

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying table counts: %w", err)
	}
	defer rows.Close()

	var results []TableRowCount
	for rows.Next() {
		var r TableRowCount
		if err := rows.Scan(&r.Name, &r.RowCount); err != nil {
			return nil, fmt.Errorf("scanning table count: %w", err)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// GetTableColumns returns the column definitions for a given schema.table.
func GetTableColumns(ctx context.Context, db *sql.DB, schemaTable string) ([]TableColumn, error) {
	schema, table := splitSchemaTable(schemaTable)

	query := `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
		ORDER BY ORDINAL_POSITION`

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query, sql.Named("schema", schema), sql.Named("table", table))
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}
	defer rows.Close()

	var cols []TableColumn
	for rows.Next() {
		var c TableColumn
		var nullable string
		if err := rows.Scan(&c.Name, &c.DataType, &nullable, &c.OrdinalPos); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}
		c.IsNullable = nullable == "YES"
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// splitSchemaTable splits "schema.table" into its parts. Defaults to "dbo" if no dot.
func splitSchemaTable(schemaTable string) (string, string) {
	for i, ch := range schemaTable {
		if ch == '.' {
			return schemaTable[:i], schemaTable[i+1:]
		}
	}
	return "dbo", schemaTable
}
