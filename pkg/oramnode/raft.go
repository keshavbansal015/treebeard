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
	currentEvictionCount int
	storageID            int
}

type beginReadPathData struct {
	paths     []int
	storageID int
}

type oramNodeFSM struct {
	unfinishedEviction   *beginEvictionData // unfinished eviction
	unfinishedEvictionMu sync.Mutex
	unfinishedReadPath   *beginReadPathData // unfinished read path
	unfinishedReadPathMu sync.Mutex
	evictionCountMap     map[int]int // map of storage id to number of evictions
}

func (fsm *oramNodeFSM) String() string {
	out := fmt.Sprintln("oramNodeFSM")
	out = out + fmt.Sprintf("unfinishedEviction: %v\n", fsm.unfinishedEviction)
	return out
}

func newOramNodeFSM() *oramNodeFSM {
	log.Info().Msg("Initializing new ORAM Node FSM")
	return &oramNodeFSM{
		evictionCountMap: make(map[int]int),
	}
}

func (fsm *oramNodeFSM) handleBeginEvictionCommand(currentEvictionCount int, storageID int) {
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleBeginEvictionCommand for storageID: %d, currentEvictionCount: %d", storageID, currentEvictionCount)
	fsm.unfinishedEvictionMu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleBeginEvictionCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleBeginEvictionCommand")
		fsm.unfinishedEvictionMu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleBeginEvictionCommand")
	}()

	log.Debug().Msgf("Setting unfinishedEviction to new data: currentEvictionCount=%d, storageID=%d", currentEvictionCount, storageID)
	fsm.unfinishedEviction = &beginEvictionData{currentEvictionCount, storageID}
}

func (fsm *oramNodeFSM) handleEndEvictionCommand(updatedEvictionCount int, storageID int) {
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleEndEvictionCommand for storageID: %d, updatedEvictionCount: %d", storageID, updatedEvictionCount)
	fsm.unfinishedEvictionMu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleEndEvictionCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleEndEvictionCommand")
		fsm.unfinishedEvictionMu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleEndEvictionCommand")
	}()
	log.Debug().Msg("Clearing unfinishedEviction data")
	fsm.unfinishedEviction = nil
	fsm.evictionCountMap[storageID] = updatedEvictionCount
	log.Debug().Msgf("Updated evictionCountMap for storageID %d to %d", storageID, updatedEvictionCount)
}

func (fsm *oramNodeFSM) handleBeginReadPathCommand(paths []int, storageID int) {
	tracer := otel.Tracer("")
	_, span := tracer.Start(context.Background(), "begin read path replication inside")
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleBeginReadPathCommand for storageID: %d, paths count: %d", storageID, len(paths))
	fsm.unfinishedReadPathMu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleBeginReadPathCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleBeginReadPathCommand")
		fsm.unfinishedReadPathMu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleBeginReadPathCommand")
	}()

	log.Debug().Msgf("Setting unfinishedReadPath to new data: storageID=%d", storageID)
	fsm.unfinishedReadPath = &beginReadPathData{paths, storageID}
	span.End()
}

func (fsm *oramNodeFSM) handleEndReadPathCommand() {
	log.Debug().Msgf("Aquiring lock for oramNodeFSM in handleEndReadPathCommand")
	fsm.unfinishedReadPathMu.Lock()
	log.Debug().Msgf("Aquired lock for oramNodeFSM in handleEndReadPathCommand")
	defer func() {
		log.Debug().Msgf("Releasing lock for oramNodeFSM in handleEndReadPathCommand")
		fsm.unfinishedReadPathMu.Unlock()
		log.Debug().Msgf("Released lock for oramNodeFSM in handleEndReadPathCommand")
	}()
	log.Debug().Msg("Clearing unfinishedReadPath data")
	fsm.unfinishedReadPath = nil
}

func (fsm *oramNodeFSM) Apply(rLog *raft.Log) interface{} {
	log.Debug().Msgf("Applying raft log with type: %s", rLog.Type.String())
	switch rLog.Type {
	case raft.LogCommand:
		var command Command
		err := msgpack.Unmarshal(rLog.Data, &command)
		if err != nil {
			log.Error().Err(err).Msg("could not unmarshall the command")
			return fmt.Errorf("could not unmarshall the command; %s", err)
		}
		log.Debug().Msgf("Received command of type: %s", command.Type.String())

		if command.Type == ReplicateBeginEviction {
			log.Debug().Msgf("got replication command for replicate begin eviction")
			var payload ReplicateBeginEvictionPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				log.Error().Err(err).Msg("could not unmarshall the begin eviction replication command")
				return fmt.Errorf("could not unmarshall the begin eviction replication command; %s", err)
			}
			fsm.handleBeginEvictionCommand(payload.CurrentEvictionCount, payload.StorageID)
		} else if command.Type == ReplicateEndEviction {
			log.Debug().Msgf("got replication command for replicate end eviction")
			var payload ReplicateEndEvictionPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				log.Error().Err(err).Msg("could not unmarshall the end eviction replication command")
				return fmt.Errorf("could not unmarshall the end eviction replication command; %s", err)
			}
			fsm.handleEndEvictionCommand(payload.UpdatedEvictionCount, payload.StorageID)
		} else if command.Type == ReplicateBeginReadPath {
			log.Debug().Msgf("got replication command for replicate begin read path")
			var payload ReplicateBeginReadPathPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				log.Error().Err(err).Msg("could not unmarshall the begin read path replication command")
				return fmt.Errorf("could not unmarshall the begin read path replication command; %s", err)
			}
			fsm.handleBeginReadPathCommand(payload.Paths, payload.StorageID)
		} else if command.Type == ReplicateEndReadPath {
			log.Debug().Msgf("got replication command for replicate end read path")
			fsm.handleEndReadPathCommand()
		} else {
			log.Error().Msgf("wrong command type: %s", command.Type.String())
		}
	default:
		log.Error().Msgf("unknown raft log type: %s", rLog.Type)
		return fmt.Errorf("unknown raft log type: %s", rLog.Type)
	}
	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

func (fsm *oramNodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	log.Debug().Msg("Creating noop snapshot")
	return snapshotNoop{}, nil
}

func (fsm *oramNodeFSM) Restore(rc io.ReadCloser) error {
	log.Error().Msg("FSM restore not implemented yet")
	return fmt.Errorf("not implemented yet")
}

// TODO: the logic for startRaftServer is the same for both shardNode and OramNode.
// TOOD: it can be moved to a new raft-utils package to reduce code duplication

func startRaftServer(isFirst bool, bindip string, advip string, replicaID int, raftPort int, oramNodeFSM *oramNodeFSM) (*raft.Raft, error) {
	log.Info().Msgf("Starting Raft server for replica ID: %d", replicaID)
	raftConfig := raft.DefaultConfig()
	// These should be here for the crash experiment
	// raftConfig.ElectionTimeout = 150 * time.Millisecond
	// raftConfig.HeartbeatTimeout = 150 * time.Millisecond
	// raftConfig.LeaderLeaseTimeout = 150 * time.Millisecond

	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{Output: log.Logger})
	raftConfig.LocalID = raft.ServerID(strconv.Itoa(replicaID))

	store := raft.NewInmemStore()
	log.Debug().Msg("Created in-memory log store")

	snapshots := raft.NewInmemSnapshotStore()
	log.Debug().Msg("Created in-memory snapshot store")

	bindAddr := fmt.Sprintf("%s:%d", bindip, raftPort)
	advAddr := fmt.Sprintf("%s:%d", advip, raftPort)
	tcpAdvertiseAddr, err := net.ResolveTCPAddr("tcp", advAddr)
	if err != nil {
		log.Error().Err(err).Msgf("could not resolve tcp addr: %s", advAddr)
		return nil, fmt.Errorf("could not resolve tcp addr; %s", err)
	}
	log.Debug().Msgf("Resolved advertise address: %s", tcpAdvertiseAddr.String())

	transport, err := raft.NewTCPTransport(bindAddr, tcpAdvertiseAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		log.Error().Err(err).Msgf("could not create tcp transport for bind address: %s", bindAddr)
		return nil, fmt.Errorf("could not create tcp transport; %s", err)
	}
	log.Debug().Msgf("Created TCP transport with bind address: %s", transport.LocalAddr().String())

	r, err := raft.NewRaft(raftConfig, oramNodeFSM, store, store, snapshots, transport)
	if err != nil {
		log.Error().Err(err).Msg("could not create raft instance")
		return nil, fmt.Errorf("could not create raft instance; %s", err)
	}
	log.Info().Msg("Raft instance created successfully")

	// This node becomes the cluster bootstraper if it is the first node and no joinAddr is specified
	if isFirst {
		log.Info().Msg("Bootstrapping Raft cluster as the first node")
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
