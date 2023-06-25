package oramnode

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type CommandType int

const (
	ReplicateOffsetList CommandType = iota
)

type Command struct {
	Type      CommandType
	RequestID string
	Payload   []byte
}

type ReplicateOffsetListPayload struct {
	OffsetList []int
}

func newReplicateOffsetListCommand(requestID string, offsetList []int) ([]byte, error) {
	payload, err := msgpack.Marshal(
		&ReplicateOffsetListPayload{
			OffsetList: offsetList,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the offsetList and beginReadPath replication payload %s", err)
	}
	command, err := msgpack.Marshal(
		&Command{
			Type:      ReplicateOffsetList,
			RequestID: requestID,
			Payload:   payload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the the offsetList and beginReadPath replication command %s", err)
	}
	return command, nil
}
