package client

import (
	"fmt"
	"os"
)

func WriteOutputToFile(outputFilePath string, responseCount []ResponseCount) error {
	file, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, count := range responseCount {
		throughput := float64(count.readOperations + count.writeOperations)
		file.WriteString(fmt.Sprintf("Throughput: %f\n", throughput))
	}
	return nil
}
