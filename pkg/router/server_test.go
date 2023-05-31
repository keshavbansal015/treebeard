package router

import (
	"testing"
)

type whereToForwardTest struct {
	r                     *routerServer
	block                 string
	expectedShardNodeDest int
}

func createTestRouterServer(shardNodeRPCClientsCount int) (r *routerServer) {
	r = &routerServer{}
	r.shardNodeRPCClients = make(map[int]ReplicaRPCClientMap)
	for i := 0; i < shardNodeRPCClientsCount; i++ {
		r.shardNodeRPCClients[i] = make(ReplicaRPCClientMap)
	}
	return r
}

var testCases = []whereToForwardTest{
	{createTestRouterServer(1), "a", 0},
	{createTestRouterServer(1), "b", 0},
	{createTestRouterServer(2), "a", 0},
	{createTestRouterServer(2), "b", 1},
	{createTestRouterServer(2), "c", 0},
	{createTestRouterServer(2), "d", 1},
	{createTestRouterServer(2), "e", 0},
	{createTestRouterServer(3), "a", 1},
	{createTestRouterServer(3), "b", 1},
	{createTestRouterServer(3), "c", 2},
	{createTestRouterServer(3), "d", 1},
	{createTestRouterServer(3), "e", 2},
}

func TestWhereToForward(t *testing.T) {
	for _, test := range testCases {
		r := test.r
		output := r.whereToForward(test.block)
		if output != test.expectedShardNodeDest {
			t.Errorf("Block \"%s\" is getting forwarded to shard node number %d while it should go to shard node number %d",
				test.block, output, test.expectedShardNodeDest)
		}
	}
}
