package client

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	routerpb "github.com/keshavbansal015/treebeard/api/router"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/rpc"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ReadResponse struct {
	block   string
	value   string
	latency time.Duration
	err     error
}

type WriteResponse struct {
	block   string
	success bool
	latency time.Duration
	err     error
}

type client struct {
	rateLimit        *RateLimit
	tracer           trace.Tracer
	routerRPCClients RouterClients
	requests         []Request
}

func NewClient(rateLimit *RateLimit, tracer trace.Tracer, routerRPCClients RouterClients, requests []Request) *client {
	log.Debug().Msg("Creating new client.")
	return &client{rateLimit: rateLimit, tracer: tracer, routerRPCClients: routerRPCClients, requests: requests}
}

func (c *client) WaitForStorageToBeReady(redisEndpoints []config.RedisEndpoint, parameters config.Parameters) error {
	log.Debug().Msg("Waiting for storage to be ready.")
	for _, redisEndpoint := range redisEndpoints {
		redisClient := redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%d", redisEndpoint.IP, redisEndpoint.Port)})
		for {
			log.Debug().Msg("Checking Redis DB size.")
			time.Sleep(100 * time.Millisecond)
			dbsize, err := redisClient.DBSize(context.Background()).Result()
			if err != nil {
				log.Error().Msgf("Failed to get DB size from redis; %v", err)
				return err
			}

			expectedSize := (int64((math.Pow(float64(parameters.Shift+1), float64(parameters.TreeHeight)))) - 1) * 2
			log.Debug().Msgf("Current DB size: %d, Expected size: %d", dbsize, expectedSize)

			if dbsize == expectedSize {
				log.Debug().Msg("Redis storage is ready.")
				break
			}
		}
	}
	return nil
}

func (c *client) asyncRead(block string, routerRPCClient RouterRPCClient, readResponseChannel chan ReadResponse) {
	log.Debug().Msgf("Starting async read for block %s", block)
	c.rateLimit.Acquire()
	ctx, span := c.tracer.Start(context.Background(), "client read request")
	startTime := time.Now()
	value, err := routerRPCClient.Read(ctx, block)
	latency := time.Since(startTime)
	log.Debug().Msgf("Got value %s for block %s with latency %v", value, block, latency)
	span.End()
	c.rateLimit.Release()
	if err != nil {
		readResponseChannel <- ReadResponse{block: block, value: "", err: fmt.Errorf("failed to call Read block %s on router; %v", block, err)}
	} else if value == "" {
		readResponseChannel <- ReadResponse{block: block, value: "", latency: latency, err: nil}
	} else {
		readResponseChannel <- ReadResponse{block: block, value: value, latency: latency, err: nil}
	}
	log.Debug().Msgf("Finished async read for block %s", block)
}

func (c *client) asyncWrite(block string, newValue string, routerRPCClient RouterRPCClient, writeResponseChannel chan WriteResponse) {
	log.Debug().Msgf("Starting async write for block %s with new value %s", block, newValue)
	c.rateLimit.Acquire()
	ctx, span := c.tracer.Start(context.Background(), "client write request")
	startTime := time.Now()
	value, err := routerRPCClient.Write(ctx, block, newValue)
	latency := time.Since(startTime)
	log.Debug().Msgf("Got success %v for block %s with latency %v", value, block, latency)
	span.End()
	c.rateLimit.Release()
	if err != nil {
		writeResponseChannel <- WriteResponse{block: block, success: false, err: fmt.Errorf("failed to call Write block %s on router; %v", block, err)}
	} else {
		writeResponseChannel <- WriteResponse{block: block, success: value, latency: latency, err: nil}
	}
	log.Debug().Msgf("Finished async write for block %s", block)
}

// TODO: I can add a counter channel to know about the operations that we sent

// sendRequestsForever cancels remaining operations and returns when the context is cancelled
func (c *client) SendRequestsForever(ctx context.Context, readResponseChannel chan ReadResponse, writeResponseChannel chan WriteResponse) {
	log.Debug().Msg("Starting to send requests forever.")
	for _, request := range c.requests {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context cancelled, stopping SendRequestsForever.")
			return
		default:
			routerRPCClient := c.routerRPCClients.GetRandomRouter()
			if request.OperationType == Read {
				log.Debug().Msgf("Sending read request for block %s", request.Block)
				go c.asyncRead(request.Block, routerRPCClient, readResponseChannel)
			} else if request.OperationType == Write {
				log.Debug().Msgf("Sending write request for block %s", request.Block)
				go c.asyncWrite(request.Block, request.NewValue, routerRPCClient, writeResponseChannel)
			}
		}
	}
	log.Debug().Msg("Finished sending all requests from trace file.")
}

type ResponseStatus struct {
	readOperations  int
	writeOperations int
	latencies       []time.Duration
}

// getResponsesForever cancels remaining operations and returns when the context is cancelled
// returns the number of read and write operations over fixed intervals in the duration
func (c *client) GetResponsesForever(ctx context.Context, readResponseChannel chan ReadResponse, writeResponseChannel chan WriteResponse) []ResponseStatus {
	log.Debug().Msg("Starting to get responses forever.")
	readOperations, writeOperations := 0, 0
	var latencies []time.Duration
	var responseCounts []ResponseStatus
	timout := time.After(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context cancelled, stopping GetResponsesForever.")
			return responseCounts
		case <-timout:
			log.Debug().Msgf("Timeout reached. Appending response status: reads=%d, writes=%d", readOperations, writeOperations)
			responseCounts = append(responseCounts, ResponseStatus{readOperations, writeOperations, latencies})
			readOperations, writeOperations = 0, 0
			latencies = nil
			timout = time.After(1 * time.Second)
		default:
		}
		select {
		case readResponse := <-readResponseChannel:
			if readResponse.err != nil {
				log.Error().Msgf(readResponse.err.Error())
			} else {
				log.Debug().Msgf("Sucess in Read of block %s. Got value: %v\n", readResponse.block, readResponse.value)
				readOperations++
				latencies = append(latencies, readResponse.latency)
			}
		case writeResponse := <-writeResponseChannel:
			if writeResponse.err != nil {
				log.Error().Msgf(writeResponse.err.Error())
			} else {
				log.Debug().Msgf("Finished writing block %s. Success: %v\n", writeResponse.block, writeResponse.success)
				writeOperations++
				latencies = append(latencies, writeResponse.latency)
			}
		default:
		}
	}
}

type RouterRPCClient struct {
	ClientAPI routerpb.RouterClient
	Conn      *grpc.ClientConn
}

type RouterClients map[int]RouterRPCClient

func (r RouterClients) GetRandomRouter() RouterRPCClient {
	log.Debug().Msg("Getting random router.")
	routersLen := len(r)
	randomRouterIndex := rand.Intn(routersLen)
	randomRouter := r[randomRouterIndex]
	log.Debug().Msgf("Selected router with ID %d", randomRouter.Conn.Target())
	return randomRouter
}

func (c *RouterRPCClient) Read(ctx context.Context, block string) (value string, err error) {
	log.Debug().Msgf("Sending read request for block %s", block)
	reply, err := c.ClientAPI.Read(ctx,
		&routerpb.ReadRequest{Block: block})
	if err != nil {
		log.Error().Msgf("Failed to send read request for block %s: %v", block, err)
		return "", err
	}
	log.Debug().Msgf("Received read response for block %s", block)
	return reply.Value, nil
}

func (c *RouterRPCClient) Write(ctx context.Context, block string, value string) (success bool, err error) {
	log.Debug().Msgf("Sending write request for block %s with value %s", block, value)
	reply, err := c.ClientAPI.Write(ctx,
		&routerpb.WriteRequest{Block: block, Value: value})
	if err != nil {
		log.Error().Msgf("Failed to send write request for block %s: %v", block, err)
		return false, err
	}
	log.Debug().Msgf("Received write response for block %s. Success: %v", block, reply.Success)
	return reply.Success, nil
}

func StartRouterRPCClients(endpoints []config.RouterEndpoint) (RouterClients, error) {
	log.Debug().Msgf("Starting router RPC clients with endpoints %v", endpoints)
	clients := make(map[int]RouterRPCClient)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		log.Debug().Msgf("Starting router client on %s", serverAddr)
		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(rpc.ContextPropagationUnaryClientInterceptor()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt64), grpc.MaxCallSendMsgSize(math.MaxInt64)),
		)
		if err != nil {
			log.Error().Msgf("Failed to dial server %s: %v", serverAddr, err)
			return nil, err
		}
		clientAPI := routerpb.NewRouterClient(conn)
		clients[endpoint.ID] = RouterRPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	log.Debug().Msgf("Successfully started %d router clients.", len(clients))
	return clients, nil
}
