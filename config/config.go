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
		GRPCPort       int    `yaml:"grpc_port"`
		StoragePath    string `yaml:"storage_path"`
		DeadLetterPath string `yaml:"dead_letter_path"`
		MaxQueueSize      int    `yaml:"max_queue_size"`
		AckTimeoutSeconds int    `yaml:"ack_timeout_seconds"`
	} `yaml:"broker"`
	CircuitBreaker struct {
		Threshold      int `yaml:"threshold"`
		TimeoutSeconds int `yaml:"timeout_seconds"`
	} `yaml:"circuit_breaker"`
	Dispatcher struct {
		Targets        []TargetConfig `yaml:"targets"`
		MaxRetries     int            `yaml:"max_retries"`
		BaseInterval   int            `yaml:"base_interval_seconds"`
		WorkerCount    int            `yaml:"worker_count"`
	} `yaml:"dispatcher"`
	Gatekeeper struct {
		RateLimitRPS   float64  `yaml:"rate_limit_rps"`
		RateLimitBurst int      `yaml:"rate_limit_burst"`
		MaxPayloadSize int64    `yaml:"max_payload_size"`
		AllowedDomains []string `yaml:"allowed_domains"`
	} `yaml:"gatekeeper"`
}

type TargetConfig struct {
	Name    string            `yaml:"name" json:"name"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
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
