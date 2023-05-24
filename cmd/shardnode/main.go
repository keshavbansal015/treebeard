package main

import (
	"flag"
	"log"

	shardnode "github.com/dsg-uwaterloo/oblishard/pkg/shardnode"
)

// Usage: ./shardnode -shardnodeid=<shardnodeid> -port=<port>
func main() {
	shardNodeID := flag.Int("shardnodeid", 0, "shardnode id, starting consecutively from zero")
	port := flag.Int("port", 0, "node port")
	flag.Parse()
	if *port == 0 {
		log.Fatalf("The port should be provided with the -port flag")
	}
	shardnode.StartRPCServer(*shardNodeID, *port)
}
