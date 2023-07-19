package oramnode

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
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vmihailenco/msgpack/v5"
)

type beginEvictionData struct {
	path      int
	storageID int
}

type oramNodeFSM struct {
	mu sync.Mutex

	offsetListMap      map[string][]int   // map of block to offsetList
	unfinishedEviction *beginEvictionData // unfinished eviction
}

func (fsm *oramNodeFSM) String() string {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	out := fmt.Sprintln("oramNodeFSM")
	out = out + fmt.Sprintf("offsetListMap: %v\n", fsm.offsetListMap)
	out = out + fmt.Sprintf("unfinishedEviction: %v\n", fsm.unfinishedEviction)
	return out
}

func newOramNodeFSM() *oramNodeFSM {
	return &oramNodeFSM{offsetListMap: make(map[string][]int)}
}

func (fsm *oramNodeFSM) handleOffsetListReplicationCommand(block string, offsetList []int) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.offsetListMap[block] = offsetList
}

func (fsm *oramNodeFSM) handleDeleteOffsetListReplicationCommand(block string) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	delete(fsm.offsetListMap, block)
}

func (fsm *oramNodeFSM) handleBeginEvictionCommand(path int, storageID int) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.unfinishedEviction = &beginEvictionData{path, storageID}
}

func (fsm *oramNodeFSM) handleEndEvictionCommand() {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.unfinishedEviction = nil
}

func (fsm *oramNodeFSM) Apply(rLog *raft.Log) interface{} {
	switch rLog.Type {
	case raft.LogCommand:
		var command Command
		err := msgpack.Unmarshal(rLog.Data, &command)
		if err != nil {
			return fmt.Errorf("could not unmarshall the command; %s", err)
		}
		requestID := command.RequestID
		if command.Type == ReplicateOffsetList {
			log.Println("got replication command for replicate offsetList")
			var payload ReplicateOffsetListPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the offsetList replication command; %s", err)
			}
			fsm.handleOffsetListReplicationCommand(requestID, payload.OffsetList)
		} else if command.Type == ReplicateDeleteOffsetList {
			log.Println("got replication command for replicate delete offsetList")
			fsm.handleDeleteOffsetListReplicationCommand(requestID)
		} else if command.Type == ReplicateBeginEviction {
			log.Println("got replication command for replicate begin eviction")
			var payload ReplicateBeginEvictionPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the offsetList replication command; %s", err)
			}
			fsm.handleBeginEvictionCommand(payload.Path, payload.StorageID)
		} else if command.Type == ReplicateEndEviction {
			log.Println("got replication command for replicate end eviction")
			fsm.handleEndEvictionCommand()
		} else {
			fmt.Println("wrong command type")
		}
	default:
		return fmt.Errorf("unknown raft log type: %s", rLog.Type)
	}
	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

func (fsm *oramNodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO: implement
	return snapshotNoop{}, nil
}

func (fsm *oramNodeFSM) Restore(rc io.ReadCloser) error {
	// TODO: implement
	return fmt.Errorf("not implemented yet") //TODO: implement
}

// TODO: the logic for startRaftServer is the same for both shardNode and OramNode.
// TOOD: it can be moved to a new raft-utils package to reduce code duplication

func startRaftServer(isFirst bool, replicaID int, raftPort int, oramNodeFSM *oramNodeFSM) (*raft.Raft, error) {
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

	r, err := raft.NewRaft(raftConfig, oramNodeFSM, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance; %s", err)
	}

	// This node becomes the cluster bootstraper if it is the first node and no joinAddr is specified
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
