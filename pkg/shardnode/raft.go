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
	requestLog      map[string][]string   // map of block to requesting requestIDs
	pathMap         map[string]int        // map of requestID to new path
	storageIDMap    map[string]int        // map of requestID to new storageID
	stash           map[string]stashState // map of block to stashState
	stashMu         sync.Mutex
	responseChannel sync.Map                 // map of requestId to their channel for receiving response map[string] chan string
	acks            map[string][]string      // map of requestID to array of blocks
	nacks           map[string][]string      // map of requestID to array of blocks
	positionMap     map[string]positionState // map of block to positionState
	positionMapMu   sync.RWMutex
	raftNode        RaftNodeWIthState
}

func newShardNodeFSM() *shardNodeFSM {
	return &shardNodeFSM{
		requestLog:      make(map[string][]string),
		pathMap:         make(map[string]int),
		storageIDMap:    make(map[string]int),
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
	out = out + fmt.Sprintf("stash: %v\n", fsm.stash)
	out = out + fmt.Sprintf("responseChannel: %v\n", fsm.responseChannel)
	out = out + fmt.Sprintf("acks: %v\n", fsm.acks)
	out = out + fmt.Sprintf("nacks: %v\n", fsm.nacks)
	out = out + fmt.Sprintf("position map: %v\n", fsm.positionMap)
	return out
}

func (fsm *shardNodeFSM) handleBatchReplicateRequestAndPathAndStorage(r BatchReplicateRequestAndPathAndStoragePayload) (isFirstMap map[string]bool) {
	isFirstMap = make(map[string]bool)
	for _, r := range r.Requests {
		fsm.requestLog[r.RequestedBlock] = append(fsm.requestLog[r.RequestedBlock], r.RequestID)
		fsm.pathMap[r.RequestID] = r.Path
		fsm.storageIDMap[r.RequestID] = r.StorageID
		if len(fsm.requestLog[r.RequestedBlock]) == 1 {
			isFirstMap[r.RequestID] = true
		} else {
			isFirstMap[r.RequestID] = false
		}
	}
	return isFirstMap
}

func (fsm *shardNodeFSM) handleReplicateResponse(r ReplicateResponsePayload) string {
	requestID := r.RequestID

	fsm.stashMu.Lock()
	stashState, exists := fsm.stash[r.RequestedBlock]
	if exists {
		if r.OpType == Write {
			stashState.logicalTime++
			stashState.value = r.NewValue
			fsm.stash[r.RequestedBlock] = stashState
		}
	} else {
		response := r.Response
		stashState := fsm.stash[r.RequestedBlock]
		if r.OpType == Read {
			stashState.value = response
			fsm.stash[r.RequestedBlock] = stashState
		} else if r.OpType == Write {
			stashState.value = r.NewValue
			fsm.stash[r.RequestedBlock] = stashState
		}
	}
	stashValue := fsm.stash[r.RequestedBlock].value
	fsm.stashMu.Unlock()
	fsm.positionMapMu.Lock()
	fsm.positionMap[r.RequestedBlock] = positionState{path: fsm.pathMap[requestID], storageID: fsm.storageIDMap[requestID]}
	fsm.positionMapMu.Unlock()
	if fsm.raftNode.State() == raft.Leader {
		for i := len(fsm.requestLog[r.RequestedBlock]) - 1; i >= 1; i-- { // We don't need to send the response to the first request
			log.Debug().Msgf("Sending response to concurrent request number %d in requestLog for block %s", i, r.RequestedBlock)
			timeout := time.After(5 * time.Second) // TODO: think about this in the batching scenario
			responseChan, exists := fsm.responseChannel.Load(fsm.requestLog[r.RequestedBlock][i])
			if !exists {
				log.Fatal().Msgf("response channel for request %s does not exist", fsm.requestLog[r.RequestedBlock][i])
			}
			select {
			case <-timeout:
				log.Error().Msgf("timeout in sending response to concurrent request number %d in requestLog for block %s", i, r.RequestedBlock)
				continue
			case responseChan.(chan string) <- stashValue:
				log.Debug().Msgf("sent response to concurrent request number %d in requestLog for block %s", i, r.RequestedBlock)
				delete(fsm.pathMap, fsm.requestLog[r.RequestedBlock][i])
				delete(fsm.storageIDMap, fsm.requestLog[r.RequestedBlock][i])
				fsm.responseChannel.Delete(fsm.requestLog[r.RequestedBlock][i])
			}
		}
	}
	delete(fsm.pathMap, requestID)
	delete(fsm.storageIDMap, requestID)
	fsm.responseChannel.Delete(requestID)
	delete(fsm.requestLog, r.RequestedBlock)
	return stashValue
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
	fsm.stashMu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
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
		if command.Type == BatchReplicateRequestAndPathAndStorageCommand {
			log.Debug().Msgf("got replication command for replicate request")
			var requestReplicationPayload BatchReplicateRequestAndPathAndStoragePayload
			err := msgpack.Unmarshal(command.Payload, &requestReplicationPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the request replication command; %s", err)
			}
			return fsm.handleBatchReplicateRequestAndPathAndStorage(requestReplicationPayload)
		} else if command.Type == ReplicateResponseCommand {
			log.Debug().Msgf("got replication command for replicate response")
			var responseReplicationPayload ReplicateResponsePayload
			err := msgpack.Unmarshal(command.Payload, &responseReplicationPayload)
			if err != nil {
				return fmt.Errorf("could not unmarshall the response replication command; %s", err)
			}
			return fsm.handleReplicateResponse(responseReplicationPayload)
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
