package utils

import (
	"fmt"

	"github.com/x-sushant-x/miniKafka/config"
)

func GetCurrentNodeClusterData(brokerID string) (config.Node, bool) {
	for _, node := range config.ClusterNodes.Nodes {
		if node.ID == brokerID {
			return node, true
		}
	}

	return config.Node{}, false
}

func BuildRaftConfigMap() map[string]string {
	m := map[string]string{}

	for _, node := range config.ClusterNodes.Nodes {
		m[node.ID] = fmt.Sprintf("%s:%s", node.Host, node.RaftPort)
	}

	return m
}
