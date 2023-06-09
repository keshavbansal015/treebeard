package main

import (
	"flag"
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/leadernotif"
)

func main() {
	port := flag.Int("port", 0, "node port")
	flag.Parse()
	if *port == 0 {
		log.Fatalf("The port should be provided with the -port flag")
	}
	leadernotif.StartRPCServer(*port)
}
