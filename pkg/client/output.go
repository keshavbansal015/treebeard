package client

import (
	"fmt"
	"os"
)

func WriteOutputToFile(outputFilePath string, throughput float64, latency float64) error {
	file, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	file.WriteString(fmt.Sprintf("Throughput: %f\n", throughput))
	file.WriteString(fmt.Sprintf("Average latency in ms: %f\n", latency))
	return nil
}
