/*
	This config won't be used for now. Just adding it for later use when I will work on my miniKafka project.
*/

package utils

import (
	"os"

	"gopkg.in/yaml.v3"
)

var Config Configuration

type Configuration struct {
	MessageMaxSize int `yaml:"message_max_size"`
}

func LoadConfig(path string) {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	if err := yaml.Unmarshal(data, &Config); err != nil {
		panic(err)
	}
}
