package oramnode

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type CommandType int

const (
	ReplicateBeginEviction CommandType = iota
	ReplicateEndEviction
)

type Command struct {
	Type      CommandType
	RequestID string
	Payload   []byte
}

type ReplicateOffsetListPayload struct {
	OffsetList []int
}

type ReplicateBeginEvictionPayload struct {
	Path      int
	StorageID int
}

func newReplicateBeginEvictionCommand(path int, storageID int) ([]byte, error) {
	payload, err := msgpack.Marshal(
		&ReplicateBeginEvictionPayload{
			Path:      path,
			StorageID: storageID,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshall payload for the begin eviction command; %s", err)
	}

	command, err := msgpack.Marshal(
		&Command{
			Type:      ReplicateBeginEviction,
			RequestID: "", // I should move requestID from the command to the payload
			Payload:   payload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the begin eviction command; %s", err)
	}
	return command, nil
}

func newReplicateEndEvictionCommand() ([]byte, error) {
	command, err := msgpack.Marshal(
		&Command{
			Type:      ReplicateEndEviction,
			RequestID: "",
			Payload:   []byte{},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the end eviction command; %s", err)
	}
	return command, nil
}
