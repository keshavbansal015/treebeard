package oramnode

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel"
)

type beginEvictionData struct {
	paths     []int
	storageID int
}

type beginReadPathData struct {
	paths     []int
	storageID int
}

type oramNodeFSM struct {
	mu sync.Mutex

	unfinishedEviction *beginEvictionData // unfinished eviction
	unfinishedReadPath *beginReadPathData // unfinished read path
}

func (fsm *oramNodeFSM) String() string {
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in String")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in String")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in String")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in String")
	}()

	out := fmt.Sprintln("oramNodeFSM")
	out = out + fmt.Sprintf("unfinishedEviction: %v\n", fsm.unfinishedEviction)
	return out
}

func newOramNodeFSM() *oramNodeFSM {
	return &oramNodeFSM{}
}

func (fsm *oramNodeFSM) handleBeginEvictionCommand(paths []int, storageID int) {
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleBeginEvictionCommand")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleBeginEvictionCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleBeginEvictionCommand")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleBeginEvictionCommand")
	}()

	fsm.unfinishedEviction = &beginEvictionData{paths, storageID}
}

func (fsm *oramNodeFSM) handleEndEvictionCommand() {
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleEndEvictionCommand")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleEndEvictionCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleEndEvictionCommand")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleEndEvictionCommand")
	}()
	fsm.unfinishedEviction = nil
}

func (fsm *oramNodeFSM) handleBeginReadPathCommand(paths []int, storageID int) {
	tracer := otel.Tracer("")
	_, span := tracer.Start(context.Background(), "begin read path replication inside")
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleBeginReadPathCommand")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleBeginReadPathCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleBeginReadPathCommand")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleBeginReadPathCommand")
	}()

	fsm.unfinishedReadPath = &beginReadPathData{paths, storageID}
	span.End()
}

func (fsm *oramNodeFSM) handleEndReadPathCommand() {
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleEndReadPathCommand")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleEndReadPathCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleEndReadPathCommand")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleEndReadPathCommand")
	}()
	fsm.unfinishedReadPath = nil
}

func (fsm *oramNodeFSM) Apply(rLog *raft.Log) interface{} {
	switch rLog.Type {
	case raft.LogCommand:
		var command Command
		err := msgpack.Unmarshal(rLog.Data, &command)
		if err != nil {
			return fmt.Errorf("could not unmarshall the command; %s", err)
		}
		if command.Type == ReplicateBeginEviction {
			log.Debug().Msgf("got replication command for replicate begin eviction")
			var payload ReplicateBeginEvictionPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the begin eviction replication command; %s", err)
			}
			fsm.handleBeginEvictionCommand(payload.Paths, payload.StorageID)
		} else if command.Type == ReplicateEndEviction {
			log.Debug().Msgf("got replication command for replicate end eviction")
			fsm.handleEndEvictionCommand()
		} else if command.Type == ReplicateBeginReadPath {
			log.Debug().Msgf("got replication command for replicate begin read path")
			var payload ReplicateBeginReadPathPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the begin read path replication command; %s", err)
			}
			fsm.handleBeginReadPathCommand(payload.Paths, payload.StorageID)
		} else if command.Type == ReplicateEndReadPath {
			log.Debug().Msgf("got replication command for replicate end read path")
			fsm.handleEndReadPathCommand()
		} else {
			log.Error().Msgf("wrong command type")
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
	return snapshotNoop{}, nil
}

func (fsm *oramNodeFSM) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("not implemented yet")
}

// TODO: the logic for startRaftServer is the same for both shardNode and OramNode.
// TOOD: it can be moved to a new raft-utils package to reduce code duplication

func startRaftServer(isFirst bool, ip string, replicaID int, raftPort int, raftDir string, oramNodeFSM *oramNodeFSM) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{Output: log.Logger})
	raftConfig.LocalID = raft.ServerID(strconv.Itoa(replicaID))

	store := raft.NewInmemStore()

	snapshots := raft.NewInmemSnapshotStore()

	raftAddr := fmt.Sprintf("%s:%d", ip, raftPort)
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
