package main

import (
	"flag"
	"log"

	oramnode "github.com/dsg-uwaterloo/oblishard/pkg/oramnode"
)

// Usage: ./oramnode -oramnodeid=<oramnodeid> -rpcport=<rpcport> -replicaid=<replicaid> -raftport=<raftport> -joinaddr=<ip:port>
func main() {
	oramNodeID := flag.Int("oramnodeid", 0, "oramnode id, starting consecutively from zero")
	replicaID := flag.Int("replicaid", 0, "replica id, starting consecutively from zero")
	rpcPort := flag.Int("rpcport", 0, "node rpc port")
	raftPort := flag.Int("raftport", 0, "node raft port")
	joinAddr := flag.String("joinaddr", "", "the address of the initial raft node, which bootstraped the cluster")
	flag.Parse()
	if *rpcPort == 0 {
		log.Fatalf("The rpc port should be provided with the -rpcport flag")
	}
	if *raftPort == 0 {
		log.Fatalf("The raft port should be provided with the -raftport flag")
	}

	oramnode.StartServer(*oramNodeID, *rpcPort, *replicaID, *raftPort, *joinAddr)
}
