package shardnode

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

type ReplicateResponsePayload struct {
	RequestedBlock string
	Response       string
	IsLeader       bool
	NewValue       string
	OpType         OperationType
}
