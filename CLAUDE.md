# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
make build          # Build binary to bin/scalesync
make run ARGS="…"   # Run via go run with ldflags
make test           # go test ./... -v -race
make test-cover     # Tests + HTML coverage report
make lint           # golangci-lint run ./...
make fmt            # gofmt -s -w .
make vet            # go vet ./...
make tidy           # go mod tidy
make docker-build   # Build Docker image
```

Run a single test: `go test ./internal/config -run TestLoadDefaults -v`

Note: `-race` requires CGO. On Windows without a C compiler, drop the `-race` flag.

## Architecture

This is a CLI tool built with **Cobra** (commands) and **Viper** (configuration).

```
cmd/scalesync/main.go   → entry point, calls cli.Execute()
internal/cli/            → Cobra command definitions (root.go, version.go)
internal/config/         → Viper-based config loading (YAML + env vars)
pkg/                     → Public library code (importable by external projects)
```

- `internal/` is not importable outside this module — all implementation goes here.
- `pkg/` is for code intentionally exposed as a library.
- New CLI subcommands go in `internal/cli/` — add a file, create a `cobra.Command`, register it with `rootCmd.AddCommand()` in an `init()` function.

## Configuration

Viper loads config in this priority order: env vars (`SCALESYNC_` prefix) > `./config.yaml` > `~/.scalesync/config.yaml` > defaults.

## Version Injection

Build-time vars (`Version`, `CommitSHA`, `BuildDate`) are injected via `-ldflags` in the Makefile. Reference them from `internal/cli.Version`, etc.
