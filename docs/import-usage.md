# CSV Import Command

The `scalesync import` command bulk-inserts CSV data into an Azure SQL table using concurrent workers and the `mssql.CopyIn` bulk copy API.

## Prerequisites

A `.env` file (or custom path via `--env`) with target database credentials:

```
TARGET_SERVER=your-server.database.windows.net
TARGET_DATABASE=your-database
TARGET_USERNAME=your-user
TARGET_DB_PASSWORD=your-password
```

## Interactive Mode

Run with no flags to be guided through each step:

```bash
scalesync import
```

The command will:

1. **Select CSV file** — scans the current directory for `.csv` files and presents a list to choose from.
2. **Connect** — loads `.env` and connects to the target database.
3. **Select target table** — queries all tables and presents a searchable list. Start typing to filter.
4. **Map columns** — matches CSV headers to table columns (case-insensitive). Prints matched/skipped counts.
5. **Confirm** — prompts `y/N` before inserting.
6. **Import** — reads CSV in batches, inserts via worker pool, displays a progress bar.
7. **Summary** — prints total rows, duration, throughput, and error count.

## Non-Interactive Mode

Pass `--file`, `--table`, and `--yes` to skip all prompts:

```bash
scalesync import \
  --file ./data.csv \
  --table dbo.MY_TABLE \
  --yes
```

| Flag | Description | Default |
|------|-------------|---------|
| `--file` | Path to CSV file (skips file selection prompt) | *(interactive)* |
| `--table` | Target table as `schema.name` (skips table selection prompt) | *(interactive)* |
| `-y, --yes` | Skip the confirmation prompt | `false` |
| `--env` | Path to `.env` file | `.env` |
| `--batch-size` | Rows per bulk-copy batch | `1000` |
| `--workers` | Number of parallel insert workers | `4` |

### Examples

Import with defaults:

```bash
scalesync import --file ./orders.csv --table dbo.ORDERS -y
```

Tune batch size and workers for a large file:

```bash
scalesync import \
  --file ./large_export.csv \
  --table dbo.TRANSACTIONS \
  --batch-size 5000 \
  --workers 8 \
  --yes
```

Use a different `.env` file:

```bash
scalesync import \
  --file ./data.csv \
  --table dbo.MY_TABLE \
  --env ./config/.env.prod \
  --yes
```

## Mixed Mode

You can provide some flags and let the command prompt for the rest. For example, specify the file but choose the table interactively:

```bash
scalesync import --file ./data.csv
```

Or specify the table but pick the CSV interactively:

```bash
scalesync import --table dbo.IA_WORK_INSTRUCTION
```

## Column Mapping

- CSV headers are matched to table columns **case-insensitively**.
- **Unmatched CSV columns** are skipped with a warning.
- **Unmatched non-nullable table columns** cause an error before any data is inserted.
- Empty CSV values become `NULL` for nullable columns.
- Datetime values in ISO 8601 format (`2025-01-02T15:04:05`) are automatically converted.
- Numeric and bit columns are coerced from their string representation.

## Helper: List Tables

To see available tables before running an import:

```bash
scalesync list-tables
scalesync list-tables --env ./other.env
```
