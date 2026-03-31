package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Broker struct {
		Host        string `yaml:"host"`
		Port        int    `yaml:"port"`
		HTTPPort    int    `yaml:"http_port"`
		GRPCPort    int    `yaml:"grpc_port"`
		StoragePath string `yaml:"storage_path"`
	} `yaml:"broker"`
	CircuitBreaker struct {
		Threshold      int `yaml:"threshold"`
		TimeoutSeconds int `yaml:"timeout_seconds"`
	} `yaml:"circuit_breaker"`
}

func LoadConfig(path string) (*Config, error) {
	config := &Config{}
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(file, config)
	return config, err
}
