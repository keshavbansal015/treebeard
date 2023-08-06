package oramnode

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type CommandType int

const (
	ReplicateBeginEviction CommandType = iota
	ReplicateEndEviction
	ReplicateBeginReadPath
	ReplicateEndReadPath
)

type Command struct {
	Type    CommandType
	Payload []byte
}

type ReplicateBeginEvictionPayload struct {
	Paths     []int
	StorageID int
}

type ReplicateBeginReadPathPayload struct {
	Paths     []int
	StorageID int
}

func newReplicateBeginEvictionCommand(paths []int, storageID int) ([]byte, error) {
	payload, err := msgpack.Marshal(
		&ReplicateBeginEvictionPayload{
			Paths:     paths,
			StorageID: storageID,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshall payload for the begin eviction command; %s", err)
	}

	command, err := msgpack.Marshal(
		&Command{
			Type:    ReplicateBeginEviction,
			Payload: payload,
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
			Type:    ReplicateEndEviction,
			Payload: []byte{},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the end eviction command; %s", err)
	}
	return command, nil
}

func newReplicateBeginReadPathCommand(paths []int, storageID int) ([]byte, error) {
	payload, err := msgpack.Marshal(
		&ReplicateBeginReadPathPayload{
			Paths:     paths,
			StorageID: storageID,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshall payload for the begin read path command; %s", err)
	}

	command, err := msgpack.Marshal(
		&Command{
			Type:    ReplicateBeginReadPath,
			Payload: payload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the begin read path command; %s", err)
	}
	return command, nil
}

func newReplicateEndReadPathCommand() ([]byte, error) {
	command, err := msgpack.Marshal(
		&Command{
			Type:    ReplicateEndReadPath,
			Payload: []byte{},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not marshal the end read path command; %s", err)
	}
	return command, nil
}
