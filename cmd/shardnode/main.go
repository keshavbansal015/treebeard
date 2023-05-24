package main

import (
	"flag"
	"log"

	shardnode "github.com/dsg-uwaterloo/oblishard/pkg/shardnode"
)

// Usage: ./shardnode -id=<nodeid> -port=<port>
func main() {
	shardNodeID := flag.Int("id", 0, "node id as an integer")
	port := flag.Int("port", 0, "node port")
	flag.Parse()
	if *port == 0 {
		log.Fatalf("The port should be provided with the -port flag")
	}
	shardnode.StartRPCServer(*shardNodeID, *port)
}
