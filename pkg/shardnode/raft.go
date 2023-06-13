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

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
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

	stash             map[string]string //map of block to value
	stashLogicalTimes map[string]int    //map of block to logical time

	responseChannel map[string]chan string //map of requestId to their channel for receiving response
}

func newShardNodeFSM() *shardNodeFSM {
	return &shardNodeFSM{
		requestLog:        make(map[string][]string),
		pathMap:           make(map[string]int),
		storageIDMap:      make(map[string]int),
		responseMap:       make(map[string]string),
		stash:             make(map[string]string),
		stashLogicalTimes: make(map[string]int),
		responseChannel:   make(map[string]chan string),
	}
}

func (fsm *shardNodeFSM) handleReplicateRequestAndPathAndStorage(requestID string, r ReplicateRequestAndPathAndStoragePayload) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.requestLog[r.RequestedBlock] = append(fsm.requestLog[r.RequestedBlock], requestID)
	fsm.pathMap[requestID] = r.Path
	fsm.storageIDMap[requestID] = r.StorageID
}

func (fsm *shardNodeFSM) handleLocalReplicaChanges(block string, requestID string, newValue string, opType OperationType, isLeader bool) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	_, exists := fsm.stash[block]
	if exists {
		fsm.stashLogicalTimes[block]++
		if opType == Write {
			fsm.stash[block] = newValue
		}
	} else {
		response := fsm.responseMap[requestID]
		if opType == Read {
			fsm.stash[block] = response
		} else if opType == Write {
			fsm.stash[block] = newValue
		}
		if isLeader {
			for _, waitingRequestID := range fsm.requestLog[block] {
				fsm.responseChannel[waitingRequestID] <- fsm.stash[block]
			}
		}
	}
	if isLeader {
		fsm.responseChannel[requestID] <- fsm.stash[block]
	}
}

func (fsm *shardNodeFSM) handleReplicateResponse(requestID string, r ReplicateResponsePayload) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.responseMap[requestID] = r.Response
	go fsm.handleLocalReplicaChanges(r.RequestedBlock, requestID, r.NewValue, r.OpType, r.IsLeader)
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
			fsm.handleReplicateResponse(requestID, responseReplicationPayload)
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
