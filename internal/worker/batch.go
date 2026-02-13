package worker

import (
	"strconv"
	"strings"
	"time"

	"github.com/walkerscm/scaleSyncGo/internal/database"
)

// datetime formats the CSV may contain (tried in order)
var dateTimeFormats = []string{
	"2006-01-02T15:04:05.000000",
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.000000",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// isDateTimeType returns true if the SQL data type is a date/time variant.
func isDateTimeType(dt string) bool {
	switch strings.ToLower(dt) {
	case "datetime", "datetime2", "smalldatetime", "date", "time":
		return true
	}
	return false
}

// isNumericType returns true if the SQL data type is numeric.
func isNumericType(dt string) bool {
	switch strings.ToLower(dt) {
	case "int", "bigint", "smallint", "tinyint",
		"decimal", "numeric", "money", "smallmoney",
		"float", "real":
		return true
	}
	return false
}

// ConvertBatch converts raw CSV string rows into [][]interface{} suitable for
// bulk insert, using the column mapping to pick the right CSV indices and
// convert values to appropriate Go types for each SQL column type.
func ConvertBatch(rows [][]string, mapping []database.ColumnMapping) [][]interface{} {
	result := make([][]interface{}, len(rows))

	for i, row := range rows {
		converted := make([]interface{}, len(mapping))
		for j, m := range mapping {
			if m.CSVIndex < len(row) {
				val := strings.TrimSpace(row[m.CSVIndex])
				if val == "" {
					if m.DBColumn.IsNullable {
						converted[j] = nil
					} else {
						converted[j] = val
					}
				} else {
					converted[j] = coerceValue(val, m.DBColumn.DataType)
				}
			} else {
				converted[j] = nil
			}
		}
		result[i] = converted
	}

	return result
}

// coerceValue converts a non-empty CSV string to the appropriate Go type.
func coerceValue(val, dataType string) interface{} {
	if isDateTimeType(dataType) {
		for _, layout := range dateTimeFormats {
			if t, err := time.Parse(layout, val); err == nil {
				return t
			}
		}
		// Couldn't parse — pass as string, let the driver try
		return val
	}

	if isNumericType(dataType) {
		// Try int first, then float
		if iv, err := strconv.ParseInt(val, 10, 64); err == nil {
			return iv
		}
		if fv, err := strconv.ParseFloat(val, 64); err == nil {
			return fv
		}
		return val
	}

	if strings.EqualFold(dataType, "bit") {
		switch strings.ToLower(val) {
		case "1", "true", "yes", "y":
			return true
		case "0", "false", "no", "n":
			return false
		}
	}

	return val
}
