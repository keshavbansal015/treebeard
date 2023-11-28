package shardnode

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

type RaftNodeWIthState interface {
	State() raft.RaftState
}

type stashState struct {
	value         string
	logicalTime   int
	waitingStatus bool
}

type positionState struct {
	path      int
	storageID int
}

func (p positionState) isPathInPaths(paths []int) bool {
	for _, path := range paths {
		if p.path == path {
			return true
		}
	}
	return false
}

type shardNodeFSM struct {
	requestLog      map[string][]string // map of block to requesting requestIDs
	requestLogMu    sync.Mutex
	pathMap         map[string]int // map of requestID to new path
	pathMapMu       sync.Mutex
	storageIDMap    map[string]int // map of requestID to new storageID
	storageIDMapMu  sync.Mutex
	responseMap     map[string]string // map of requestID to response map[string]string
	responseMapMu   sync.Mutex
	stash           map[string]stashState // map of block to stashState
	stashMu         sync.Mutex
	responseChannel sync.Map            // map of requestId to their channel for receiving response map[string] chan string
	acks            map[string][]string // map of requestID to array of blocks
	acksMu          sync.Mutex
	nacks           map[string][]string // map of requestID to array of blocks
	nacksMu         sync.Mutex
	positionMap     map[string]positionState // map of block to positionState
	positionMapMu   sync.RWMutex
	raftNode        RaftNodeWIthState
	raftNodeMu      sync.Mutex
}

func newShardNodeFSM() *shardNodeFSM {
	return &shardNodeFSM{
		requestLog:      make(map[string][]string),
		pathMap:         make(map[string]int),
		storageIDMap:    make(map[string]int),
		responseMap:     make(map[string]string),
		stash:           make(map[string]stashState),
		responseChannel: sync.Map{},
		acks:            make(map[string][]string),
		nacks:           make(map[string][]string),
		positionMap:     make(map[string]positionState),
	}
}

func (fsm *shardNodeFSM) String() string {
	out := fmt.Sprintln("ShardNodeFSM")
	out = out + fmt.Sprintf("requestLog: %v\n", fsm.requestLog)
	out = out + fmt.Sprintf("pathMap: %v\n", fsm.pathMap)
	out = out + fmt.Sprintf("storageIDMap: %v\n", fsm.storageIDMap)
	out = out + fmt.Sprintf("responseMap: %v\n", fsm.responseMap)
	out = out + fmt.Sprintf("stash: %v\n", fsm.stash)
	out = out + fmt.Sprintf("responseChannel: %v\n", fsm.responseChannel)
	out = out + fmt.Sprintf("acks: %v\n", fsm.acks)
	out = out + fmt.Sprintf("nacks: %v\n", fsm.nacks)
	out = out + fmt.Sprintf("position map: %v\n", fsm.positionMap)
	return out
}

func (fsm *shardNodeFSM) handleReplicateRequestAndPathAndStorage(requestID string, r ReplicateRequestAndPathAndStoragePayload) (isFirst bool) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
	fsm.requestLogMu.Lock()
	fsm.pathMapMu.Lock()
	fsm.storageIDMapMu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
		fsm.requestLogMu.Unlock()
		fsm.pathMapMu.Unlock()
		fsm.storageIDMapMu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
	}()

	fsm.requestLog[r.RequestedBlock] = append(fsm.requestLog[r.RequestedBlock], requestID)
	fsm.pathMap[requestID] = r.Path
	fsm.storageIDMap[requestID] = r.StorageID
	if len(fsm.requestLog[r.RequestedBlock]) == 1 {
		isFirst = true
	} else {
		isFirst = false
	}
	return isFirst
}

type localReplicaChangeHandlerFunc func(requestID string, r ReplicateResponsePayload)

// It handles the response replication changes locally on each raft replica.
// The leader doesn't wait for this to finish to return success for the response replication command.
func (fsm *shardNodeFSM) handleLocalResponseReplicationChanges(requestID string, r ReplicateResponsePayload) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleLocalResponseReplicationChanges")
	fsm.stashMu.Lock()
	fsm.responseMapMu.Lock()
	fsm.positionMapMu.Lock()
	fsm.pathMapMu.Lock()
	fsm.storageIDMapMu.Lock()
	fsm.requestLogMu.Lock()
	fsm.raftNodeMu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleLocalResponseReplicationChanges")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateResponse")
		fsm.stashMu.Unlock()
		fsm.responseMapMu.Unlock()
		fsm.positionMapMu.Unlock()
		fsm.pathMapMu.Unlock()
		fsm.storageIDMapMu.Unlock()
		fsm.requestLogMu.Unlock()
		fsm.raftNodeMu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleReplicateResponse")
	}()

	stashState, exists := fsm.stash[r.RequestedBlock]
	if exists {
		if r.OpType == Write {
			stashState.logicalTime++
			stashState.value = r.NewValue
			fsm.stash[r.RequestedBlock] = stashState
		}
	} else {
		response := fsm.responseMap[requestID]
		stashState := fsm.stash[r.RequestedBlock]
		if r.OpType == Read {
			stashState.value = response
			fsm.stash[r.RequestedBlock] = stashState
		} else if r.OpType == Write {
			stashState.value = r.NewValue
			fsm.stash[r.RequestedBlock] = stashState
		}
	}
	if fsm.raftNode.State() == raft.Leader {
		fsm.positionMap[r.RequestedBlock] = positionState{path: fsm.pathMap[requestID], storageID: fsm.storageIDMap[requestID]}
		for i := len(fsm.requestLog[r.RequestedBlock]) - 1; i >= 0; i-- {
			timeout := time.After(5 * time.Second) // TODO: think about this in the batching scenario
			responseChan, _ := fsm.responseChannel.Load(fsm.requestLog[r.RequestedBlock][i])
			select {
			case <-timeout:
				log.Error().Msgf("timeout in sending response to concurrent requests")
				continue
			case responseChan.(chan string) <- fsm.stash[r.RequestedBlock].value:
				delete(fsm.pathMap, fsm.requestLog[r.RequestedBlock][i])
				delete(fsm.storageIDMap, fsm.requestLog[r.RequestedBlock][i])
				fsm.responseChannel.Delete(fsm.requestLog[r.RequestedBlock][i])
				fsm.requestLog[r.RequestedBlock] = append(fsm.requestLog[r.RequestedBlock][:i], fsm.requestLog[r.RequestedBlock][i+1:]...)
			}
		}
	}
	delete(fsm.responseMap, requestID)
}

func (fsm *shardNodeFSM) handleReplicateResponse(requestID string, r ReplicateResponsePayload, f localReplicaChangeHandlerFunc) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleReplicateResponse")
	fsm.responseMapMu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateResponse")

	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateResponse")
		fsm.responseMapMu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleReplicateResponse")
	}()

	fsm.responseMap[requestID] = r.Response
	go f(requestID, r)
}

func (fsm *shardNodeFSM) handleReplicateSentBlocks(r ReplicateSentBlocksPayload) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleReplicateSentBlocks")
	fsm.stashMu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateSentBlocks")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateSentBlocks")
		fsm.stashMu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleReplicateSentBlocks")
	}()

	for _, block := range r.SentBlocks {
		stashState := fsm.stash[block]
		stashState.logicalTime = 0
		stashState.waitingStatus = true
		fsm.stash[block] = stashState
	}
}

// It keeps the nacked blocks and deletes not changed acked blocks.
// If an acked block was changed during the eviction, it will keep it.
func (fsm *shardNodeFSM) handleLocalAcksNacksReplicationChanges(requestID string) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
	fsm.acksMu.Lock()
	fsm.nacksMu.Lock()
	fsm.stashMu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
		fsm.acksMu.Unlock()
		fsm.nacksMu.Unlock()
		fsm.stashMu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
	}()

	for _, block := range fsm.acks[requestID] {
		stashState := fsm.stash[block]
		if stashState.logicalTime == 0 {
			delete(fsm.stash, block)
		}
	}
	for _, block := range fsm.nacks[requestID] {
		stashState := fsm.stash[block]
		stashState.waitingStatus = false
		fsm.stash[block] = stashState
	}
	delete(fsm.acks, requestID)
	delete(fsm.nacks, requestID)
}

func (fsm *shardNodeFSM) handleReplicateAcksNacks(r ReplicateAcksNacksPayload) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleReplicateAcksNacks")
	fsm.acksMu.Lock()
	fsm.nacksMu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateAcksNacks")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateAcksNacks")
		fsm.acksMu.Unlock()
		fsm.nacksMu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleReplicateAcksNacks")
	}()

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
			log.Debug().Msgf("got replication command for replicate request")
			var requestReplicationPayload ReplicateRequestAndPathAndStoragePayload
			err := msgpack.Unmarshal(command.Payload, &requestReplicationPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the request replication command; %s", err)
			}
			return fsm.handleReplicateRequestAndPathAndStorage(requestID, requestReplicationPayload)
		} else if command.Type == ReplicateResponseCommand {
			log.Debug().Msgf("got replication command for replicate response")
			var responseReplicationPayload ReplicateResponsePayload
			err := msgpack.Unmarshal(command.Payload, &responseReplicationPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the response replication command; %s", err)
			}
			fsm.handleReplicateResponse(requestID, responseReplicationPayload, fsm.handleLocalResponseReplicationChanges)
		} else if command.Type == ReplicateSentBlocksCommand {
			log.Debug().Msgf("got replication command for replicate sent blocks")
			var replicateSentBlocksPayload ReplicateSentBlocksPayload
			err := msgpack.Unmarshal(command.Payload, &replicateSentBlocksPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the sent blocks replication command; %s", err)
			}
			fsm.handleReplicateSentBlocks(replicateSentBlocksPayload)
		} else if command.Type == ReplicateAcksNacksCommand {
			log.Debug().Msgf("got replication command for replicate acks/nacks")
			var payload ReplicateAcksNacksPayload
			err := msgpack.Unmarshal(command.Payload, &payload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the acks/nacks replication command; %s", err)
			}
			fsm.handleReplicateAcksNacks(payload)
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

func (fsm *shardNodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (fsm *shardNodeFSM) Restore(rc io.ReadCloser) error {
	return fmt.Errorf("not implemented yet")
}

func startRaftServer(isFirst bool, ip string, replicaID int, raftPort int, shardshardNodeFSM *shardNodeFSM) (*raft.Raft, error) {

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

	r, err := raft.NewRaft(raftConfig, shardshardNodeFSM, store, store, snapshots, transport)
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
