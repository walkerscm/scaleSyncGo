# ScaleSyncGo

A CLI tool for scaling and synchronization operations.

## Quick Start

```bash
# Build
make build

# Run
./bin/scalesync version

# Test
make test
```

## Configuration

ScaleSyncGo reads configuration from (in order of precedence):
1. Environment variables prefixed with `SCALESYNC_`
2. `config.yaml` in the current directory
3. `~/.scalesync/config.yaml`

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SCALESYNC_LOG_LEVEL` | `info` | Log level: debug, info, warn, error |
| `SCALESYNC_OUTPUT` | `text` | Output format: text, json |

## Development

Requires Go 1.22+.

```bash
make build       # Build binary to bin/
make test        # Run all tests
make test-cover  # Run tests with coverage report
make lint        # Run golangci-lint
make fmt         # Format code
make vet         # Run go vet
make tidy        # Tidy go modules
make clean       # Remove build artifacts
```

## Docker

```bash
make docker-build
docker compose up
```
