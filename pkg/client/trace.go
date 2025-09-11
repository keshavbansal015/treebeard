package client

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

const (
	Read = iota
	Write
)

type Request struct {
	Block         string
	OperationType int
	NewValue      string
}

func padBlockValue(blockValue string, blockSizeBytes int) string {
	log.Debug().Msgf("Padding block value '%s' to a size of %d bytes", blockValue, blockSizeBytes)
	if len(blockValue) < blockSizeBytes {
		paddedValue := strings.Repeat("0", blockSizeBytes-len(blockValue)) + blockValue
		log.Debug().Msgf("Padded value is: %s", paddedValue)
		return paddedValue
	}
	log.Debug().Msgf("Block value already meets or exceeds block size. No padding needed.")
	return blockValue
}

func ReadTraceFile(traceFilePath string, blockSizeBytes int) ([]Request, error) {
	log.Debug().Msgf("Starting to read trace file: %s", traceFilePath)
	file, err := os.Open(traceFilePath)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to open trace file: %s", traceFilePath)
		return nil, err
	}
	defer func() {
		log.Debug().Msgf("Closing trace file: %s", traceFilePath)
		file.Close()
	}()

	var requests []Request

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		line := scanner.Text()
		log.Debug().Msgf("Processing line %d: '%s'", lineCount, line)
		tokens := strings.Split(line, " ")
		if tokens[0] == "GET" {
			log.Debug().Msgf("Detected 'GET' operation on line %d.", lineCount)
			if len(tokens) != 2 {
				log.Error().Msgf("Read request at line %d has incorrect token count (%d). Expected 2.", lineCount, len(tokens))
				return nil, fmt.Errorf("read request should have the operation type and block id")
			}
			requests = append(requests, Request{Block: tokens[1], OperationType: Read})
			log.Debug().Msgf("Added READ request for block: %s", tokens[1])
		} else if tokens[0] == "SET" {
			log.Debug().Msgf("Detected 'SET' operation on line %d.", lineCount)
			if len(tokens) != 3 {
				log.Error().Msgf("Write request at line %d has incorrect token count (%d). Expected 3.", lineCount, len(tokens))
				return nil, fmt.Errorf("read request should have the operation type, block id, and new value")
			}
			newValue := padBlockValue(tokens[2], blockSizeBytes)
			requests = append(requests, Request{Block: tokens[1], OperationType: Write, NewValue: newValue})
			log.Debug().Msgf("Added WRITE request for block: %s with new value: %s", tokens[1], newValue)
		} else {
			log.Error().Msgf("Unsupported operation '%s' at line %d.", tokens[0], lineCount)
			return nil, fmt.Errorf("only READ and WRITE are supported in the trace file")
		}
	}
	log.Debug().Msgf("Finished processing trace file. Read %d requests in total.", len(requests))
	return requests, nil
}
