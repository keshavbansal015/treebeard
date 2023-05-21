package utils

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v3"
)

type Endpoint struct {
	IP   string
	Port int
	ID   int
}

type Config struct {
	Endpoints []Endpoint
}

func ReadEndpoints(path string) ([]Endpoint, error) {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}
	return config.Endpoints, nil
}
