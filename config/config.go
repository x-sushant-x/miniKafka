package config

import (
	"encoding/json"
	"os"
)

var Config Configuration
var ClusterNodes ClusterNodesData

type Configuration struct {
	Broker                      Broker `json:"broker"`
	TopicsStorageDir            string `json:"topics_storage_dir"`
	RetentionTimeDays           int    `json:"retention_time_days"`
	CleanupCheckIntervalSeconds int    `json:"cleanup_check_interval_seconds"`
}

type Broker struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port string `json:"port"`
}

type ClusterNodesData struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

func LoadConfig() error {
	data, err := os.ReadFile("config.json")
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &Config)
}

func LoadClusterConfig() error {
	data, err := os.ReadFile("cluster_nodes.json")
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &ClusterNodes)
}
