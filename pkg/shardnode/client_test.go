package shardnode

import (
	"reflect"
	"testing"
)

func TestGetRandomOramNodeReplicaMapReturnsRandomClientExistingInOramNodeMap(t *testing.T) {
	oramNodes := RPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {},
			1: {},
		},
		1: map[int]oramNodeRPCClient{
			0: {},
		},
	}
	random := oramNodes.getRandomOramNodeReplicaMap()
	for _, replicaMap := range oramNodes {
		if reflect.DeepEqual(random, replicaMap) {
			return
		}
	}
	t.Errorf("the random map does not exist")
}
