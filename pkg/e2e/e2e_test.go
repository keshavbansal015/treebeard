package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/keshavbansal015/treebeard/pkg/client"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/oramnode"
	"github.com/keshavbansal015/treebeard/pkg/router"
	"github.com/keshavbansal015/treebeard/pkg/shardnode"
	"github.com/rs/zerolog/log"
)

func startRouter() {
	log.Debug().Msg("Starting router...")
	shardNodeEndpoints, err := config.ReadShardNodeEndpoints("./configs/shardnode_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	log.Debug().Msgf("Read %d shard node endpoints from config file.", len(shardNodeEndpoints))

	rpcClients, err := router.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}
	log.Debug().Msg("Successfully started shard node RPC clients.")
	router.StartRPCServer("localhost", rpcClients, 0, 8745, config.Parameters{EpochTime: 10})
	log.Debug().Msg("Router RPC server started.")
}

func startShardNode(replicaID int, rpcPort int, raftPort int, joinAddr string) {
	log.Debug().Msgf("Starting shard node with replicaID %d, rpcPort %d, raftPort %d, joinAddr %s...", replicaID, rpcPort, raftPort, joinAddr)
	oramNodeEndpoints, err := config.ReadOramNodeEndpoints("./configs/oramnode_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read oram node endpoints from yaml file; %v", err)
	}
	log.Debug().Msgf("Read %d oram node endpoints from config file.", len(oramNodeEndpoints))

	rpcClients, err := shardnode.StartOramNodeRPCClients(oramNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with oram node servers; %v", err)
	}
	log.Debug().Msg("Successfully started oram node RPC clients.")

	parameters, err := config.ReadParameters("./configs/parameters.yaml")
	if err != nil {
		log.Fatal().Msgf("Failed to read parameters from yaml file; %v", err)
	}
	log.Debug().Msgf("Read parameters from config: %+v", parameters)

	redisEndpoints := []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}}
	log.Debug().Msgf("Using Redis endpoint: %+v", redisEndpoints)

	shardnode.StartServer(0, "localhost", "localhost", rpcPort, replicaID, raftPort, joinAddr, rpcClients, parameters, redisEndpoints, "../../configs")
	log.Debug().Msgf("Shard node server for replicaID %d started.", replicaID)
}

func startOramNode(replicaID int, rpcPort int, raftPort int, joinAddr string) {
	log.Debug().Msgf("Starting ORAM node with replicaID %d, rpcPort %d, raftPort %d, joinAddr %s...", replicaID, rpcPort, raftPort, joinAddr)
	shardNodeEndpoints, err := config.ReadShardNodeEndpoints("./configs/shardnode_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	log.Debug().Msgf("Read %d shard node endpoints from config file.", len(shardNodeEndpoints))

	rpcClients, err := oramnode.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}
	log.Debug().Msg("Successfully started shard node RPC clients for ORAM node.")

	parameters, err := config.ReadParameters("./configs/parameters.yaml")
	if err != nil {
		log.Fatal().Msgf("Failed to read parameters from yaml file; %v", err)
	}
	log.Debug().Msgf("Read parameters from config: %+v", parameters)
	oramnode.StartServer(0, "localhost", "localhost", rpcPort, replicaID, raftPort, joinAddr, rpcClients, []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}}, parameters)
	log.Debug().Msgf("ORAM node server for replicaID %d started.", replicaID)
}

// It assumes that the redis service is running on the default port (6379)
func startTestSystem() {
	log.Debug().Msg("Starting the entire test system...")
	go startRouter()
	go startShardNode(0, 8748, 3124, "")
	log.Debug().Msg("Waiting 4 seconds for shard node leader election.")
	time.Sleep(4 * time.Second) // This is a bad of way of ensuring the leader is elected
	go startShardNode(1, 8749, 3125, "127.0.0.1:8748")
	go startShardNode(2, 8750, 3126, "127.0.0.1:8748")
	go startOramNode(0, 8751, 1415, "")
	log.Debug().Msg("Waiting 4 seconds for ORAM node leader election.")
	time.Sleep(4 * time.Second) // This is a bad of way of ensuring the leader is elected
	go startOramNode(1, 8752, 1416, "127.0.0.1:8751")
	go startOramNode(2, 8753, 1417, "127.0.0.1:8751")
	log.Debug().Msg("All system components started.")
	// TODO: kill the go routines, maybe by using cancel contexts
}

// TODO: make the tests better
func TestSimpleRequestsReturnCorrectResponses(t *testing.T) {
	log.Debug().Msg("Starting TestSimpleRequestsReturnCorrectResponses...")
	startTestSystem()
	routerEndpoints, err := config.ReadRouterEndpoints("./configs/router_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read router endpoints from yaml file; %v", err)
	}
	log.Debug().Msgf("Read %d router endpoints from config file.", len(routerEndpoints))

	rpcClients, err := client.StartRouterRPCClients(routerEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to start clients; %v", err)
	}
	log.Debug().Msg("Router RPC clients started.")
	routerRPCClient := rpcClients.GetRandomRouter()
	log.Debug().Msg("Acquired a random router RPC client.")

	log.Debug().Msgf("Writing value 'meow' to key 'cat'...")
	writeValue, err := routerRPCClient.Write(context.Background(), "cat", "meow")
	if err != nil {
		log.Error().Err(err).Msg("Unable to write data to the system.")
		t.Errorf("unable to write data to the system; %v", err)
	}
	if writeValue == false {
		log.Error().Msg("Write operation returned false, expected true.")
		t.Errorf("write should return success: true")
	}
	log.Debug().Msg("Write operation successful.")

	log.Debug().Msgf("Reading value for key 'cat'...")
	readValue, err := routerRPCClient.Read(context.Background(), "cat")
	if err != nil {
		log.Error().Err(err).Msg("Unable to read data from the system.")
		t.Errorf("unable to read data from the system; %v", err)
	}
	log.Debug().Msgf("Read value is '%s'.", readValue)
	if readValue != "meow" {
		log.Error().Msgf("Read value mismatch. Expected 'meow', got '%s'.", readValue)
		t.Errorf("expected read value to be meow, but it is %s", readValue)
	}
	log.Debug().Msg("Read value matches expected value.")

	log.Debug().Msg("TestSimpleRequestsReturnCorrectResponses completed.")
}
