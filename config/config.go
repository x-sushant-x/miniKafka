package config

import (
	"encoding/json"
	"os"
)

var Config Configuration

type Configuration struct {
	TopicsStorageDir            string `json:"topics_storage_dir"`
	BrokerPort                  string `json:"broker_port"`
	RetentionTimeSeconds        int    `json:"retention_time_seconds"`
	CleanupCheckIntervalSeconds int    `json:"cleanup_check_interval_seconds"`
}

func LoadConfig() error {
	data, err := os.ReadFile("config.json")
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &Config)
}
