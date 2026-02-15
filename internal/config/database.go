package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// DatabaseConfig holds the connection parameters for an Azure SQL database.
type DatabaseConfig struct {
	Server   string
	Database string
	Username string
	Password string
}

// targetEnvPrefix maps a --target value to its env var prefix.
var targetEnvPrefix = map[string]string{
	"prod": "PROD",
	"test": "TEST",
}

// ValidTargets returns the accepted --target values.
func ValidTargets() []string {
	return []string{"prod", "test"}
}

// LoadDatabaseConfig reads the .env file at envPath and returns the database
// configuration for the given target ("prod" or "test"). The env vars are
// read with the prefix {TARGET}_{FIELD}, e.g. PROD_SERVER or TEST_SERVER.
func LoadDatabaseConfig(envPath, target string) (*DatabaseConfig, error) {
	if err := godotenv.Load(envPath); err != nil {
		return nil, fmt.Errorf("loading %s: %w", envPath, err)
	}

	prefix, ok := targetEnvPrefix[strings.ToLower(target)]
	if !ok {
		return nil, fmt.Errorf("unknown target %q (valid: %s)", target, strings.Join(ValidTargets(), ", "))
	}

	cfg := &DatabaseConfig{
		Server:   os.Getenv(prefix + "_SERVER"),
		Database: os.Getenv(prefix + "_DATABASE"),
		Username: os.Getenv(prefix + "_USERNAME"),
		Password: os.Getenv(prefix + "_DB_PASSWORD"),
	}

	if cfg.Server == "" || cfg.Database == "" || cfg.Username == "" || cfg.Password == "" {
		return nil, fmt.Errorf("missing required env vars: %s_SERVER, %s_DATABASE, %s_USERNAME, %s_DB_PASSWORD",
			prefix, prefix, prefix, prefix)
	}

	return cfg, nil
}

// ConnectionString builds an ADO-style connection string for go-mssqldb.
func (c *DatabaseConfig) ConnectionString() string {
	query := url.Values{}
	query.Add("database", c.Database)
	query.Add("encrypt", "true")
	query.Add("TrustServerCertificate", "false")
	query.Add("connection timeout", "30")

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     c.Server,
		RawQuery: query.Encode(),
	}
	return u.String()
}
