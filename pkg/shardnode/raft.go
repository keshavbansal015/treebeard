package shardnode

import (
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
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
	// I'm starting with simple maps and one mutex to handle race conditions.
	// However, there are other ways to design this that might be better regarding performance:
	//     1. using different mutexes for different maps so that we just block the exact map that is having multiple access.
	//     2. using sync.Map. This takes away type safety but might have better performance.
	//        * https://medium.com/@deckarep/the-new-kid-in-town-gos-sync-map-de24a6bf7c2c
	//        * https://www.youtube.com/watch?v=C1EtfDnsdDs
	//        * https://pkg.go.dev/sync

	mu         sync.Mutex
	requestLog map[string][]string // map of block to requesting requestIDs

	pathMap      map[string]int // map of requestID to new path
	storageIDMap map[string]int // map of requestID to new storageID

	responseMap map[string]string // map of requestID to response map[string]string

	stash map[string]stashState // map of block to stashState

	responseChannel map[string]chan string // map of requestId to their channel for receiving response

	acks  map[string][]string // map of requestID to array of blocks
	nacks map[string][]string // map of requestID to array of blocks

	positionMap map[string]positionState // map of block to positionState

	raftNode RaftNodeWIthState
}

func newShardNodeFSM() *shardNodeFSM {
	return &shardNodeFSM{
		requestLog:      make(map[string][]string),
		pathMap:         make(map[string]int),
		storageIDMap:    make(map[string]int),
		responseMap:     make(map[string]string),
		stash:           make(map[string]stashState),
		responseChannel: make(map[string]chan string),
		acks:            make(map[string][]string),
		nacks:           make(map[string][]string),
		positionMap:     make(map[string]positionState),
	}
}

func (fsm *shardNodeFSM) String() string {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

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

func (fsm *shardNodeFSM) isInitialRequest(block string, requestID string) bool {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in isInitialRequest")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in isInitialRequest")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in isInitialRequest")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in isInitialRequest")
	}()

	return fsm.requestLog[block][0] == requestID
}

func (fsm *shardNodeFSM) handleReplicateRequestAndPathAndStorage(requestID string, r ReplicateRequestAndPathAndStoragePayload) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleReplicateRequestAndPathAndStorage")
	}()

	fsm.requestLog[r.RequestedBlock] = append(fsm.requestLog[r.RequestedBlock], requestID)
	fsm.pathMap[requestID] = r.Path
	fsm.storageIDMap[requestID] = r.StorageID
}

type localReplicaChangeHandlerFunc func(requestID string, r ReplicateResponsePayload)

// It handles the response replication changes locally on each raft replica.
// The leader doesn't wait for this to finish to return success for the response replication command.
func (fsm *shardNodeFSM) handleLocalResponseReplicationChanges(requestID string, r ReplicateResponsePayload) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleLocalResponseReplicationChanges")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleLocalResponseReplicationChanges")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleLocalResponseReplicationChanges")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleLocalResponseReplicationChanges")
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
		for _, waitingRequestID := range fsm.requestLog[r.RequestedBlock] {
			timout := time.After(1 * time.Second) // TODO: think about this in the batching scenario
			select {
			case <-timout:
				continue
			case fsm.responseChannel[waitingRequestID] <- fsm.stash[r.RequestedBlock].value:
				continue
			}
		}
	}
	fsm.positionMap[r.RequestedBlock] = positionState{path: fsm.pathMap[requestID], storageID: fsm.storageIDMap[requestID]}
	delete(fsm.requestLog, r.RequestedBlock)
	delete(fsm.pathMap, requestID)
	delete(fsm.storageIDMap, requestID)
	delete(fsm.responseMap, requestID)
	delete(fsm.responseChannel, requestID)
}

func (fsm *shardNodeFSM) handleReplicateResponse(requestID string, r ReplicateResponsePayload, f localReplicaChangeHandlerFunc) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleReplicateResponse")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateResponse")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateResponse")
		fsm.mu.Unlock()
		log.Debug().Msgf("Released lock for shardNodeFSM in handleReplicateResponse")
	}()

	fsm.responseMap[requestID] = r.Response
	go f(requestID, r)
}

func (fsm *shardNodeFSM) handleReplicateSentBlocks(r ReplicateSentBlocksPayload) {
	log.Debug().Msgf("Aquiring lock for shardNodeFSM in handleReplicateSentBlocks")
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateSentBlocks")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateSentBlocks")
		fsm.mu.Unlock()
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
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleLocalAcksNacksReplicationChanges")
		fsm.mu.Unlock()
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
	fsm.mu.Lock()
	log.Debug().Msgf("Aquired lock for shardNodeFSM in handleReplicateAcksNacks")
	defer func() {
		log.Debug().Msgf("Releasing lock for shardNodeFSM in handleReplicateAcksNacks")
		fsm.mu.Unlock()
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
			fsm.handleReplicateRequestAndPathAndStorage(requestID, requestReplicationPayload)
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

func (fsm *shardNodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	shardNodeSnapshot := shardNodeSnapshot{}

	shardNodeSnapshot.RequestLog = make(map[string][]string)
	for requestID, log := range fsm.requestLog {
		shardNodeSnapshot.RequestLog[requestID] = append(shardNodeSnapshot.RequestLog[requestID], log...)
	}
	shardNodeSnapshot.PathMap = make(map[string]int)
	for requestID, path := range fsm.pathMap {
		shardNodeSnapshot.PathMap[requestID] = path
	}
	shardNodeSnapshot.StorageIDMap = make(map[string]int)
	for requestID, storageID := range fsm.storageIDMap {
		shardNodeSnapshot.StorageIDMap[requestID] = storageID
	}
	shardNodeSnapshot.ResponseMap = make(map[string]string)
	for requestID, response := range fsm.responseMap {
		shardNodeSnapshot.ResponseMap[requestID] = response
	}
	shardNodeSnapshot.Stash = make(map[string]stashState)
	for block, value := range fsm.stash {
		shardNodeSnapshot.Stash[block] = value
	}

	shardNodeSnapshot.Acks = make(map[string][]string)
	for requestID, blocks := range fsm.acks {
		shardNodeSnapshot.Acks[requestID] = append(shardNodeSnapshot.Acks[requestID], blocks...)
	}
	shardNodeSnapshot.Nacks = make(map[string][]string)
	for requestID, blocks := range fsm.nacks {
		shardNodeSnapshot.Nacks[requestID] = append(shardNodeSnapshot.Nacks[requestID], blocks...)
	}

	shardNodeSnapshot.PositionMap = make(map[string]positionState)
	for block, pos := range fsm.positionMap {
		shardNodeSnapshot.PositionMap[block] = pos
	}

	return shardNodeSnapshot, nil
}

func (fsm *shardNodeFSM) Restore(rc io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	b, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("could not read snapshot from io.ReadCloser; %s", err)
	}
	var snapshot shardNodeSnapshot
	err = msgpack.Unmarshal(b, &snapshot)
	if err != nil {
		return fmt.Errorf("could not unmarshal snapshot; %s", err)
	}

	fsm.requestLog = make(map[string][]string)
	for requestID, log := range snapshot.RequestLog {
		fsm.requestLog[requestID] = append(fsm.requestLog[requestID], log...)
	}
	fsm.pathMap = make(map[string]int)
	for requestID, path := range snapshot.PathMap {
		fsm.pathMap[requestID] = path
	}
	fsm.storageIDMap = make(map[string]int)
	for requestID, storageID := range snapshot.StorageIDMap {
		fsm.storageIDMap[requestID] = storageID
	}
	fsm.responseMap = make(map[string]string)
	for requestID, response := range snapshot.ResponseMap {
		fsm.responseMap[requestID] = response
	}
	fsm.stash = make(map[string]stashState)
	for block, value := range snapshot.Stash {
		fsm.stash[block] = value
	}

	fsm.acks = make(map[string][]string)
	for requestID, blocks := range snapshot.Acks {
		fsm.acks[requestID] = append(fsm.acks[requestID], blocks...)
	}
	fsm.nacks = make(map[string][]string)
	for requestID, blocks := range snapshot.Nacks {
		fsm.nacks[requestID] = append(fsm.nacks[requestID], blocks...)
	}

	fsm.positionMap = make(map[string]positionState)
	for block, pos := range snapshot.PositionMap {
		fsm.positionMap[block] = pos
	}

	return nil
}

type shardNodeSnapshot struct {
	RequestLog   map[string][]string
	PathMap      map[string]int
	StorageIDMap map[string]int
	ResponseMap  map[string]string
	Stash        map[string]stashState
	Acks         map[string][]string
	Nacks        map[string][]string
	PositionMap  map[string]positionState
}

func (sn shardNodeSnapshot) Persist(sink raft.SnapshotSink) error {
	b, err := msgpack.Marshal(sn)
	if err != nil {
		return fmt.Errorf("could not marshal snapshot for writing to disk; %s", err)
	}
	_, err = sink.Write(b)
	if err != nil {
		return fmt.Errorf("could not write marshalled snapshot to disk; %s", err)
	}
	return sink.Close()
}
func (sn shardNodeSnapshot) Release() {}

func startRaftServer(isFirst bool, ip string, replicaID int, raftPort int, raftDir string, shardshardNodeFSM *shardNodeFSM) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{Output: log.Logger})
	raftConfig.LocalID = raft.ServerID(strconv.Itoa(replicaID))

	store, err := raftboltdb.NewBoltStore(path.Join(raftDir, "bolt"))
	if err != nil {
		return nil, fmt.Errorf("could not create the bolt store; %s", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(raftDir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create the snapshot store; %s", err)
	}

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
