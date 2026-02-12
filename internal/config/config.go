package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	LogLevel string `mapstructure:"log_level"`
	Output   string `mapstructure:"output"`
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.scalesync")

	viper.SetDefault("log_level", "info")
	viper.SetDefault("output", "text")

	viper.SetEnvPrefix("SCALESYNC")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	return &cfg, nil
}
