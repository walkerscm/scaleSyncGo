package config

import (
	"os"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	// Clear any env vars that might interfere
	os.Unsetenv("SCALESYNC_LOG_LEVEL")
	os.Unsetenv("SCALESYNC_OUTPUT")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("expected log_level=info, got %s", cfg.LogLevel)
	}

	if cfg.Output != "text" {
		t.Errorf("expected output=text, got %s", cfg.Output)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("SCALESYNC_LOG_LEVEL", "debug")
	t.Setenv("SCALESYNC_OUTPUT", "json")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("expected log_level=debug, got %s", cfg.LogLevel)
	}

	if cfg.Output != "json" {
		t.Errorf("expected output=json, got %s", cfg.Output)
	}
}
