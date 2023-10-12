package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dsg-uwaterloo/oblishard/pkg/client"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/shardnode"
	"github.com/rs/zerolog/log"
)

func startRouter() {
	shardNodeEndpoints, err := config.ReadShardNodeEndpoints("./configs/shardnode_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := router.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}
	router.StartRPCServer("localhost", rpcClients, 0, 8745)
}

func startShardNode(replicaID int, rpcPort int, raftPort int, raftDir string, joinAddr string) {
	os.RemoveAll(fmt.Sprintf("sh-data-replicaid-%d", replicaID))
	err := os.MkdirAll(fmt.Sprintf("sh-data-replicaid-%d", replicaID), os.ModePerm)
	if err != nil {
		log.Fatal().Msgf("Unable to create raft directory")
	}
	oramNodeEndpoints, err := config.ReadOramNodeEndpoints("./configs/oramnode_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read oram node endpoints from yaml file; %v", err)
	}
	rpcClients, err := shardnode.StartOramNodeRPCClients(oramNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with oram node servers; %v", err)
	}
	parameters, err := config.ReadParameters("./configs/parameters.yaml")
	if err != nil {
		log.Fatal().Msgf("Failed to read parameters from yaml file; %v", err)
	}
	shardnode.StartServer(0, "localhost", rpcPort, replicaID, raftPort, raftDir, joinAddr, rpcClients, parameters, "../../configs/redis-data.txt")
}

func startOramNode(replicaID int, rpcPort int, raftPort int, raftDir string, joinAddr string) {
	os.RemoveAll(fmt.Sprintf("om-data-replicaid-%d", replicaID))
	err := os.MkdirAll(fmt.Sprintf("om-data-replicaid-%d", replicaID), os.ModePerm)
	if err != nil {
		log.Fatal().Msgf("Unable to create raft directory")
	}
	shardNodeEndpoints, err := config.ReadShardNodeEndpoints("./configs/shardnode_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := oramnode.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}
	parameters, err := config.ReadParameters("./configs/parameters.yaml")
	if err != nil {
		log.Fatal().Msgf("Failed to read parameters from yaml file; %v", err)
	}
	oramnode.StartServer(0, "localhost", rpcPort, replicaID, raftPort, raftDir, joinAddr, rpcClients, parameters)
}

// It assumes that the redis service is running on the default port (6379)
func startTestSystem() {
	go startRouter()
	go startShardNode(0, 8748, 3124, "sh-data-replicaid-0", "")
	time.Sleep(4 * time.Second) // This is a bad of way of ensuring the leader is elected
	go startShardNode(1, 8749, 3125, "sh-data-replicaid-1", "127.0.0.1:8748")
	go startShardNode(2, 8750, 3126, "sh-data-replicaid-2", "127.0.0.1:8748")
	go startOramNode(0, 8751, 1415, "om-data-replicaid-0", "")
	time.Sleep(4 * time.Second) // This is a bad of way of ensuring the leader is elected
	go startOramNode(1, 8752, 1416, "om-data-replicaid-1", "127.0.0.1:8751")
	go startOramNode(2, 8753, 1417, "om-data-replicaid-2", "127.0.0.1:8751")
	// redis?
	// TODO: kill the go routines, maybe by using cancel contexts
}

// TODO: make the tests better
func TestSimpleRequestsReturnCorrectResponses(t *testing.T) {
	startTestSystem()
	routerEndpoints, err := config.ReadRouterEndpoints("./configs/router_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read router endpoints from yaml file; %v", err)
	}

	rpcClients, err := client.StartRouterRPCClients(routerEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to start clients; %v", err)
	}
	routerRPCClient := rpcClients.GetRandomRouter()

	writeValue, err := routerRPCClient.Write(context.Background(), "cat", "meow")
	if err != nil {
		t.Errorf("unable to write data to the system; %v", err)
	}
	if writeValue == false {
		t.Errorf("wirte should return success: true")
	}

	readValue, err := routerRPCClient.Read(context.Background(), "cat")
	if err != nil {
		t.Errorf("unable to read data from the system; %v", err)
	}
	if readValue != "meow" {
		t.Errorf("expected read value to be meow, but it is %s", readValue)
	}
}
