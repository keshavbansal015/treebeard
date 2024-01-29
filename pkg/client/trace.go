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
	Block         string
	OperationType int
	NewValue      string
}

func padBlockValue(blockValue string, blockSizeBytes int) string {
	if len(blockValue) < blockSizeBytes {
		return strings.Repeat("0", blockSizeBytes-len(blockValue)) + blockValue
	}
	return blockValue
}

func ReadTraceFile(traceFilePath string, blockSizeBytes int) ([]Request, error) {
	log.Debug().Msgf("Reading trace file")
	file, err := os.Open(traceFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var requests []Request

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, " ")
		if tokens[0] == "GET" {
			if len(tokens) != 2 {
				return nil, fmt.Errorf("read request should have the operation type and block id")
			}
			requests = append(requests, Request{Block: tokens[1], OperationType: Read})
		} else if tokens[0] == "SET" {
			if len(tokens) != 3 {
				return nil, fmt.Errorf("read request should have the operation type, block id, and new value")
			}
			newValue := padBlockValue(tokens[2], blockSizeBytes)
			requests = append(requests, Request{Block: tokens[1], OperationType: Write, NewValue: newValue})
		} else {
			return nil, fmt.Errorf("only READ and WRITE are supported in the trace file")
		}
	}
	return requests, nil
}
