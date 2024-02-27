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
	sum := 0.0
	for _, count := range responseCount {
		throughput := float64(count.readOperations + count.writeOperations)
		sum += throughput
		file.WriteString(fmt.Sprintf("Throughput: %f\n", throughput))
	}
	file.WriteString(fmt.Sprintf("Average Throughput: %f\n", sum/float64(len(responseCount))))
	return nil
}
