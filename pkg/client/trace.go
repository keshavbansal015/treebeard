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

type request struct {
	Block         string
	OperationType int
	NewValue      string
}

func ReadTraceFile(traceFilePath string) ([]request, error) {
	log.Debug().Msgf("Reading trace file")
	file, err := os.Open(traceFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var requests []request

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		log.Debug().Msgf("Reading trace line: %s", line)
		tokens := strings.Split(line, " ")
		if tokens[0] == "READ" {
			if len(tokens) != 2 {
				return nil, fmt.Errorf("read request should have the operation type and block id")
			}
			requests = append(requests, request{Block: tokens[1], OperationType: Read})
		} else if tokens[0] == "WRITE" {
			if len(tokens) != 3 {
				return nil, fmt.Errorf("read request should have the operation type, block id, and new value")
			}
			requests = append(requests, request{Block: tokens[1], OperationType: Write, NewValue: tokens[2]})
		} else {
			return nil, fmt.Errorf("only READ and WRITE are supported in the trace file")
		}
	}
	return requests, nil
}
