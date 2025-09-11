package client

import (
	"fmt"
	"log" // Import the log package
	"os"
)

func WriteOutputToFile(outputFilePath string, responseCount []ResponseStatus) error {
	// Log the start of the function and the output file path.
	log.Printf("DEBUG: Starting WriteOutputToFile for path: %s", outputFilePath)

	file, err := os.Create(outputFilePath)
	if err != nil {
		// Log the error if file creation fails.
		log.Printf("ERROR: Failed to create file %s. Error: %v", outputFilePath, err)
		return err
	}
	// Defer closing the file and log it.
	defer func() {
		log.Println("DEBUG: Closing the output file.")
		file.Close()
	}()

	sum := 0.0
	experimentAverageLatency := 0.0

	// Log the number of response counts to process.
	log.Printf("DEBUG: Processing %d response counts.", len(responseCount))

	for i, count := range responseCount {
		// Log the start of processing for each item in the loop.
		log.Printf("DEBUG: Processing response count #%d", i)

		throughput := float64(count.readOperations + count.writeOperations)
		averageLatency := 0.0
		// Log the number of latencies to be averaged.
		log.Printf("DEBUG: Calculating average latency for %d latencies.", len(count.latencies))

		for _, latency := range count.latencies {
			averageLatency += float64(latency.Milliseconds())
		}

		if len(count.latencies) > 0 {
			averageLatency = averageLatency / float64(len(count.latencies))
		}

		sum += throughput
		experimentAverageLatency += averageLatency

		// Log the calculated throughput and average latency before writing.
		log.Printf("DEBUG: Calculated Throughput: %f, Average Latency: %f", throughput, averageLatency)

		file.WriteString(fmt.Sprintf("Throughput: %f\n", throughput))
		file.WriteString(fmt.Sprintf("Average Latency: %f\n", averageLatency))
	}

	// Log the final calculated average throughput and experiment average latency.
	avgThroughput := sum / float64(len(responseCount))
	avgExperimentLatency := experimentAverageLatency / float64(len(responseCount))
	log.Printf("DEBUG: Final Average Throughput: %f", avgThroughput)
	log.Printf("DEBUG: Final Experiment Average Latency: %f", avgExperimentLatency)

	file.WriteString(fmt.Sprintf("Average Throughput: %f\n", avgThroughput))
	file.WriteString(fmt.Sprintf("Experiment Average Latency: %f\n", avgExperimentLatency))

	// Log the successful completion of the function.
	log.Println("DEBUG: Successfully wrote all output to file.")
	return nil
}
