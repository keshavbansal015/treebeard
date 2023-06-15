package shardnode

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type CommandType int

const (
	ReplicateRequestAndPathAndStorageCommand CommandType = iota
	ReplicateResponseCommand
)

type Command struct {
	Type      CommandType
	RequestID string
	Payload   []byte
}

type ReplicateRequestAndPathAndStoragePayload struct {
	RequestedBlock string
	Path           int
	StorageID      int
}

func newRequestReplicationCommand(block string, requestID string) ([]byte, error) {
	requestReplicationPayload, err := msgpack.Marshal(
		&ReplicateRequestAndPathAndStoragePayload{
			RequestedBlock: block,
			Path:           0, //TODO: update to use a real path
			StorageID:      0, //TODO: update to use a real storage id
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the request, path, storage replication payload %s", err)
	}
	requestReplicationCommand, err := msgpack.Marshal(
		&Command{
			Type:      ReplicateRequestAndPathAndStorageCommand,
			RequestID: requestID,
			Payload:   requestReplicationPayload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the request, path, storage replication command %s", err)
	}
	return requestReplicationCommand, nil

}

type ReplicateResponsePayload struct {
	RequestedBlock string
	Response       string
	IsLeader       bool
	NewValue       string
	OpType         OperationType
}

func newResponseReplicationCommand(response string, requestID string, block string, newValue string, opType OperationType, isLeader bool) ([]byte, error) {
	responseReplicationPayload, err := msgpack.Marshal(
		&ReplicateResponsePayload{
			Response:       response,
			IsLeader:       isLeader,
			RequestedBlock: block,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the response replication payload; %s", err)
	}
	responseReplicationCommand, err := msgpack.Marshal(
		&Command{
			Type:      ReplicateResponseCommand,
			RequestID: requestID,
			Payload:   responseReplicationPayload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the response replication command; %s", err)
	}
	return responseReplicationCommand, nil
}
