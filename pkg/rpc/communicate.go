package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	grpc "google.golang.org/grpc"
)

type result struct {
	reply interface{}
	err   error
}

type CallFunc func(ctx context.Context, client interface{}, request interface{}, opts ...grpc.CallOption) (interface{}, error)

// TODO: move previous tests for calling all replicas to this package
func CallAllReplicas(ctx context.Context, clients []interface{}, replicaFuncs []CallFunc, request interface{}) (reply interface{}, err error) {
	responseChannel := make(chan result)
	for i, clientFunc := range replicaFuncs {
		go func(f CallFunc, client interface{}) {
			reply, err := f(ctx, client, request)
			responseChannel <- result{reply: reply, err: err}
		}(clientFunc, clients[i])
	}
	timeout := time.After(1 * time.Second)
	for {
		select {
		case result := <-responseChannel:
			log.Debug().Msgf("Received result in CallAllReplicas %v", result)
			if result.err != nil {
				log.Error().Msgf("Error in CallAllReplicas %v", result.err)
				// TODO: return leader errors, but it is hard to know because the replicas might be down and you may get other types of errors from them
				continue
			}
			log.Debug().Msgf("Returning result in CallAllReplicas %v", result.reply)
			return result.reply, nil
		case <-timeout:
			return nil, fmt.Errorf("could not read blocks from the replicas")
		}
	}
}
