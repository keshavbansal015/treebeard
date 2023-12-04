package shardnode

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type CommandType int

const (
	BatchReplicateRequestAndPathAndStorageCommand CommandType = iota
	ReplicateResponseCommand
	ReplicateSentBlocksCommand
	ReplicateAcksNacksCommand
)

type Command struct {
	Type    CommandType
	Payload []byte
}

type ReplicateRequestAndPathAndStoragePayload struct {
	RequestedBlock string
	Path           int
	StorageID      int
	RequestID      string
}

type BatchReplicateRequestAndPathAndStoragePayload struct {
	Requests []ReplicateRequestAndPathAndStoragePayload
}

func newRequestReplicationCommand(requests []ReplicateRequestAndPathAndStoragePayload) ([]byte, error) {
	batchRequestReplicationPayload, err := msgpack.Marshal(
		&BatchReplicateRequestAndPathAndStoragePayload{
			Requests: requests,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the batch request, path, storage replication payload %s", err)
	}
	requestReplicationCommand, err := msgpack.Marshal(
		&Command{
			Type:    BatchReplicateRequestAndPathAndStorageCommand,
			Payload: batchRequestReplicationPayload,
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
	NewValue       string
	OpType         OperationType
	RequestID      string
}

func newResponseReplicationCommand(response string, requestID string, block string, newValue string, opType OperationType) ([]byte, error) {
	responseReplicationPayload, err := msgpack.Marshal(
		&ReplicateResponsePayload{
			Response:       response,
			RequestedBlock: block,
			NewValue:       newValue,
			OpType:         opType,
			RequestID:      requestID,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the response replication payload; %s", err)
	}
	responseReplicationCommand, err := msgpack.Marshal(
		&Command{
			Type:    ReplicateResponseCommand,
			Payload: responseReplicationPayload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the response replication command; %s", err)
	}
	return responseReplicationCommand, nil
}

type ReplicateSentBlocksPayload struct {
	SentBlocks []string
}

func newSentBlocksReplicationCommand(sentBlocks []string) ([]byte, error) {
	payload, err := msgpack.Marshal(
		&ReplicateSentBlocksPayload{
			SentBlocks: sentBlocks,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the sent blocks replication payload; %s", err)
	}
	command, err := msgpack.Marshal(
		&Command{
			Type:    ReplicateSentBlocksCommand,
			Payload: payload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the sent blocks replication command; %s", err)
	}
	return command, nil
}

type ReplicateAcksNacksPayload struct {
	AckedBlocks  []string
	NackedBlocks []string
}

func newAcksNacksReplicationCommand(ackBlocks []string, nackBlocks []string) ([]byte, error) {
	payload, err := msgpack.Marshal(
		&ReplicateAcksNacksPayload{
			AckedBlocks:  ackBlocks,
			NackedBlocks: nackBlocks,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the acks/nacks replication payload; %s", err)
	}
	command, err := msgpack.Marshal(
		&Command{
			Type:    ReplicateAcksNacksCommand,
			Payload: payload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the sent acks/nacks replication command; %s", err)
	}
	return command, nil
}
