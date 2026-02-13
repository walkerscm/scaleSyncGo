package config

import (
	"fmt"
	"net/url"
	"os"

	"github.com/joho/godotenv"
)

// DatabaseConfig holds the connection parameters for an Azure SQL database.
type DatabaseConfig struct {
	Server   string
	Database string
	Username string
	Password string
}

// LoadDatabaseConfig reads the .env file at envPath and returns the target
// database configuration from TARGET_SERVER, TARGET_DATABASE, TARGET_USERNAME,
// and TARGET_DB_PASSWORD environment variables.
func LoadDatabaseConfig(envPath string) (*DatabaseConfig, error) {
	if err := godotenv.Load(envPath); err != nil {
		return nil, fmt.Errorf("loading %s: %w", envPath, err)
	}

	cfg := &DatabaseConfig{
		Server:   os.Getenv("TARGET_SERVER"),
		Database: os.Getenv("TARGET_DATABASE"),
		Username: os.Getenv("TARGET_USERNAME"),
		Password: os.Getenv("TARGET_DB_PASSWORD"),
	}

	if cfg.Server == "" || cfg.Database == "" || cfg.Username == "" || cfg.Password == "" {
		return nil, fmt.Errorf("missing required env vars: TARGET_SERVER, TARGET_DATABASE, TARGET_USERNAME, TARGET_DB_PASSWORD")
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
