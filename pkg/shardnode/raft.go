package shardnode

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vmihailenco/msgpack/v5"
)

type shardNodeFSM struct {
	//TODO note
	//I'm starting with simple maps and one mutex to handle race conditions.
	//However, there are other ways to design this that might be better regarding performance:
	//    1. using different mutexes for different maps so that we just block the exact map that is having multiple access.
	//    2. using sync.Map. This takes away type safety but might have better performance.
	//       * https://medium.com/@deckarep/the-new-kid-in-town-gos-sync-map-de24a6bf7c2c
	//       * https://www.youtube.com/watch?v=C1EtfDnsdDs
	//       * https://pkg.go.dev/sync

	mu         sync.Mutex
	requestLog map[string][]string //map of block to requesting requestIDs

	pathMap      map[string]int //map of requestID to new path
	storageIDMap map[string]int //map of requestID to new storageID

	responseMap map[string]string //map of requestID to response map[string]string

	//TODO: i should merge these three to a map[string]struct format
	stash              map[string]string //map of block to value
	stashLogicalTimes  map[string]int    //map of block to logical time
	stashWaitingStatus map[string]bool   //map of block to waiting status

	responseChannel map[string]chan string //map of requestId to their channel for receiving response

	acks  map[string][]string //map of requestID to array of blocks
	nacks map[string][]string //map of requestID to array of blocks
}

func newShardNodeFSM() *shardNodeFSM {
	return &shardNodeFSM{
		requestLog:         make(map[string][]string),
		pathMap:            make(map[string]int),
		storageIDMap:       make(map[string]int),
		responseMap:        make(map[string]string),
		stash:              make(map[string]string),
		stashLogicalTimes:  make(map[string]int),
		stashWaitingStatus: make(map[string]bool),
		responseChannel:    make(map[string]chan string),
		acks:               make(map[string][]string),
		nacks:              make(map[string][]string),
	}
}

func (fsm *shardNodeFSM) handleReplicateRequestAndPathAndStorage(requestID string, r ReplicateRequestAndPathAndStoragePayload) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.requestLog[r.RequestedBlock] = append(fsm.requestLog[r.RequestedBlock], requestID)
	fsm.pathMap[requestID] = r.Path
	fsm.storageIDMap[requestID] = r.StorageID
}

type localReplicaChangeHandlerFunc func(requestID string, r ReplicateResponsePayload)

func (fsm *shardNodeFSM) handleLocalResponseReplicationChanges(requestID string, r ReplicateResponsePayload) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	_, exists := fsm.stash[r.RequestedBlock]
	if exists {
		if r.OpType == Write {
			fsm.stashLogicalTimes[r.RequestedBlock]++
			fsm.stash[r.RequestedBlock] = r.NewValue
		}
	} else {
		response := fsm.responseMap[requestID]
		if r.OpType == Read {
			fsm.stash[r.RequestedBlock] = response
		} else if r.OpType == Write {
			fsm.stash[r.RequestedBlock] = r.NewValue
		}
	}
	if r.IsLeader {
		for _, waitingRequestID := range fsm.requestLog[r.RequestedBlock] {
			fsm.responseChannel[waitingRequestID] <- fsm.stash[r.RequestedBlock]
		}
		delete(fsm.requestLog, r.RequestedBlock)
	}
}

func (fsm *shardNodeFSM) handleReplicateResponse(requestID string, r ReplicateResponsePayload, f localReplicaChangeHandlerFunc) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.responseMap[requestID] = r.Response
	go f(requestID, r)
}

func (fsm *shardNodeFSM) handleReplicateSentBlocks(r ReplicateSentBlocksPayload) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	for _, block := range r.SentBlocks {
		fsm.stashLogicalTimes[block] = 0
		fsm.stashWaitingStatus[block] = true
	}
}

func (fsm *shardNodeFSM) handleLocalAcksNacksReplicationChanges(requestID string) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	for _, block := range fsm.acks[requestID] {
		if fsm.stashLogicalTimes[block] == 0 {
			delete(fsm.stash, block)
			delete(fsm.stashLogicalTimes, block)
		}
		delete(fsm.stashWaitingStatus, block)
	}
	for _, block := range fsm.nacks[requestID] {
		delete(fsm.stashWaitingStatus, block)
	}
}

func (fsm *shardNodeFSM) handleReplicateAcksNacks(r ReplicateAcksNacksPayload) {

	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	requestID := uuid.New().String()
	fsm.acks[requestID] = r.AckedBlocks
	fsm.nacks[requestID] = r.NackedBlocks

	go fsm.handleLocalAcksNacksReplicationChanges(requestID)
}

func (fsm *shardNodeFSM) Apply(rLog *raft.Log) interface{} {
	switch rLog.Type {
	case raft.LogCommand:
		var command Command
		err := msgpack.Unmarshal(rLog.Data, &command)
		if err != nil {
			return fmt.Errorf("could not unmarshall the command; %s", err)
		}
		requestID := command.RequestID
		if command.Type == ReplicateRequestAndPathAndStorageCommand {
			log.Println("got replication command for replicate request")
			var requestReplicationPayload ReplicateRequestAndPathAndStoragePayload
			err := msgpack.Unmarshal(command.Payload, &requestReplicationPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the request replication command; %s", err)
			}
			fsm.handleReplicateRequestAndPathAndStorage(requestID, requestReplicationPayload)
		} else if command.Type == ReplicateResponseCommand {
			log.Println("got replication command for replicate response")
			var responseReplicationPayload ReplicateResponsePayload
			err := msgpack.Unmarshal(command.Payload, &responseReplicationPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the response replication command; %s", err)
			}
			fsm.handleReplicateResponse(requestID, responseReplicationPayload, fsm.handleLocalResponseReplicationChanges)
		} else if command.Type == ReplicateSentBlocksCommand {
			log.Println("got replication command for replicate sent blocks")
			var replicateSentBlocksPayload ReplicateSentBlocksPayload
			err := msgpack.Unmarshal(command.Payload, &replicateSentBlocksPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the sent blocks replication command; %s", err)
			}
			fsm.handleReplicateSentBlocks(replicateSentBlocksPayload)
		} else if command.Type == ReplicateAcksNacksCommand {
			log.Println("got replication command for replicate acks/nacks")
			var payload ReplicateAcksNacksPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the acks/nacks replication command; %s", err)
			}
			fsm.handleReplicateAcksNacks(payload)
		} else {
			fmt.Println("wrong command type")
		}
	default:
		return fmt.Errorf("unknown raft log type: %s", rLog.Type)
	}
	return nil
}

func (fsm *shardNodeFSM) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("not implemented yet") //TODO: implement
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

func (fsm *shardNodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil //TODO: implement
}

func startRaftServer(isFirst bool, replicaID int, raftPort int, shardshardNodeFSM *shardNodeFSM) (*raft.Raft, error) {
	dataDir := fmt.Sprintf("data-replicaid-%d", replicaID)
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(strconv.Itoa(replicaID))

	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create the data directory; %s", err)
	}

	store, err := raftboltdb.NewBoltStore(path.Join(dataDir, "bolt"))
	if err != nil {
		return nil, fmt.Errorf("could not create the bolt store; %s", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dataDir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create the snapshot store; %s", err)
	}

	raftAddr := fmt.Sprintf("localhost:%d", raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("could not resolve tcp addr; %s", err)
	}

	transport, err := raft.NewTCPTransport(raftAddr, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport; %s", err)
	}

	r, err := raft.NewRaft(raftConfig, shardshardNodeFSM, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance; %s", err)
	}

	//This node becomes the cluster bootstraper if it is the first node and no joinAddr is specified
	if isFirst {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}
	return r, nil
}
