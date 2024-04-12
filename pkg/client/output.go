package client

import (
	"fmt"
	"os"
)

func WriteOutputToFile(outputFilePath string, responseCount []ResponseStatus) error {
	file, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	sum := 0.0
	experimentAverageLatency := 0.0

	for _, count := range responseCount {
		throughput := float64(count.readOperations + count.writeOperations)
		averageLatency := 0.0
		for _, latency := range count.latencies {
			averageLatency += float64(latency.Milliseconds())
		}
		averageLatency = averageLatency / float64(len(count.latencies))
		sum += throughput
		experimentAverageLatency += averageLatency
		file.WriteString(fmt.Sprintf("Throughput: %f\n", throughput))
		file.WriteString(fmt.Sprintf("Average Latency: %f\n", averageLatency))
	}
	file.WriteString(fmt.Sprintf("Average Throughput: %f\n", sum/float64(len(responseCount))))
	file.WriteString(fmt.Sprintf("Experiment Average Latency: %f\n", experimentAverageLatency/float64(len(responseCount))))
	return nil
}
