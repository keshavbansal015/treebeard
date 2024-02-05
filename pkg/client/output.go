package client

import (
	"fmt"
	"os"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
)

func WriteOutputToFile(outputFilePath string, throughput float64, latency float64, parameters config.Parameters) error {
	file, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	file.WriteString(parameters.String() + "\n")
	file.WriteString("############################################\n")
	file.WriteString(fmt.Sprintf("Throughput: %f\n", throughput))
	file.WriteString(fmt.Sprintf("Average latency in ms: %f\n", latency))
	return nil
}
