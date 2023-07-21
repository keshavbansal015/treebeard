package config

import (
	"os"

	yaml "gopkg.in/yaml.v3"
)

type RouterEndpoint struct {
	IP   string
	Port int
	ID   int
}

type ShardNodeEndpoint struct {
	IP        string
	Port      int
	ID        int
	ReplicaID int
}

type OramNodeEndpoint struct {
	IP        string
	Port      int
	ID        int
	ReplicaID int
}

type RouterConfig struct {
	Endpoints []RouterEndpoint
}

type ShardNodeConfig struct {
	Endpoints []ShardNodeEndpoint
}

type OramNodeConfig struct {
	Endpoints []OramNodeEndpoint
}

func ReadRouterEndpoints(path string) ([]RouterEndpoint, error) {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config RouterConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}
	return config.Endpoints, nil
}

func ReadShardNodeEndpoints(path string) ([]ShardNodeEndpoint, error) {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config ShardNodeConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}
	return config.Endpoints, nil
}

func ReadOramNodeEndpoints(path string) ([]OramNodeEndpoint, error) {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config OramNodeConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}
	return config.Endpoints, nil
}
