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
	timeout := time.After(50 * time.Second)
	var errors []error
	errorCount := 0
	for {
		select {
		case resultResponse := <-responseChannel:
			log.Debug().Msgf("Received result in CallAllReplicas %v", resultResponse)
			if resultResponse.err != nil {
				errors = append(errors, resultResponse.err)
				if len(errors) == len(clients) {
					errorCount++
					if errorCount == 50 {
						return nil, fmt.Errorf("could not read blocks from the replicas %v", errors)
					}
					time.Sleep(100 * time.Millisecond)
					errors = nil
					responseChannel = make(chan result)
					for i, clientFunc := range replicaFuncs {
						go func(f CallFunc, client interface{}) {
							reply, err := f(ctx, client, request)
							responseChannel <- result{reply: reply, err: err}
						}(clientFunc, clients[i])
					}
				}
				continue
			}
			log.Debug().Msgf("Returning result in CallAllReplicas %v", resultResponse.reply)
			return resultResponse.reply, nil
		case <-timeout:
			return nil, fmt.Errorf("timeout while waiting for all replicas to respond")
		}
	}
}
