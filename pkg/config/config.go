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

type RouterConfig struct {
	Endpoints []RouterEndpoint
}

type ShardNodeConfig struct {
	Endpoints []ShardNodeEndpoint
}

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
