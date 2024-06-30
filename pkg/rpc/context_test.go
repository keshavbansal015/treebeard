package rpc_test

import (
	"context"
	"testing"

	"github.com/dsg-uwaterloo/treebeard/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestContextPropagationUnaryServerInterceptorSendsIngoingContextToOutgoingContext(t *testing.T) {

	initCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("test", "test_metadata"))
	interceptor := rpc.ContextPropagationUnaryServerInterceptor()

	newCtx, _ := interceptor(initCtx, "", &grpc.UnaryServerInfo{}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return ctx, nil
	})

	md, _ := metadata.FromOutgoingContext(newCtx.(context.Context))
	if md["test"][0] != "test_metadata" {
		t.Errorf("Expected to see metadata on outgoing context")
	}
}
