package database

import (
	"fmt"
	"strings"
)

// ColumnMapping describes how CSV columns map to database table columns.
type ColumnMapping struct {
	// CSVIndex is the index of the column in the CSV header row.
	CSVIndex int
	// CSVName is the original CSV header name.
	CSVName string
	// DBColumn is the matching database column definition.
	DBColumn TableColumn
}

// MapResult holds the outcome of column mapping.
type MapResult struct {
	Mapped    []ColumnMapping
	Skipped   []string // CSV columns with no table match
	Unmatched []string // Non-nullable table columns with no CSV match
}

// MapColumns performs case-insensitive matching between CSV headers and table columns.
// CSV columns that don't match any table column are skipped (with a warning).
// Non-nullable table columns that have no CSV match cause an error.
func MapColumns(csvHeaders []string, tableCols []TableColumn) (*MapResult, error) {
	result := &MapResult{}

	// Build lookup: lowercase table column name → TableColumn
	lookup := make(map[string]TableColumn, len(tableCols))
	for _, tc := range tableCols {
		lookup[strings.ToLower(tc.Name)] = tc
	}

	matched := make(map[string]bool)

	for i, h := range csvHeaders {
		key := strings.ToLower(strings.TrimSpace(h))
		if tc, ok := lookup[key]; ok {
			result.Mapped = append(result.Mapped, ColumnMapping{
				CSVIndex: i,
				CSVName:  h,
				DBColumn: tc,
			})
			matched[key] = true
		} else {
			result.Skipped = append(result.Skipped, h)
		}
	}

	// Check for non-nullable table columns that have no CSV match
	for _, tc := range tableCols {
		key := strings.ToLower(tc.Name)
		if !matched[key] && !tc.IsNullable {
			result.Unmatched = append(result.Unmatched, tc.Name)
		}
	}

	if len(result.Unmatched) > 0 {
		return result, fmt.Errorf("non-nullable table columns have no matching CSV header: %s",
			strings.Join(result.Unmatched, ", "))
	}

	return result, nil
}
