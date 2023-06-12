package config

import (
	"io/ioutil" //TODO: fix this it's deprecated

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

// TODO: Refactor to remove duplication between the following functions
func ReadRouterEndpoints(path string) ([]RouterEndpoint, error) {
	yamlFile, err := ioutil.ReadFile(path)
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
	yamlFile, err := ioutil.ReadFile(path)
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
	yamlFile, err := ioutil.ReadFile(path)
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
