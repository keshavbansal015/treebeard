package rpc_test

import (
	"context"
	"testing"

	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestGetContextWithRequestIDHasUUIDInMetadata(t *testing.T) {
	ctx := rpc.GetContextWithRequestID(context.Background())
	md, _ := metadata.FromOutgoingContext(ctx)
	if len(md["requestid"]) != 1 {
		t.Errorf("Expected a value in the requestid metadata for the request but got metadata: %v", md)
	}
}

func TestGetContextWithRequestIDWhenCalledMultipleTimesGeneratesDistinctRequestIDs(t *testing.T) {
	ctx1 := rpc.GetContextWithRequestID(context.Background())
	ctx2 := rpc.GetContextWithRequestID(context.Background())
	md1, _ := metadata.FromOutgoingContext(ctx1)
	md2, _ := metadata.FromOutgoingContext(ctx2)
	if md1["requestid"][0] == md2["requestid"][0] {
		t.Errorf("Expected different values for requestid for different contexts but got: %s", md1["requestid"][0])
	}
}

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

func TestGetRequestIDFromContextReturnsRequestIDFromMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "1238394"))
	requestID, err := rpc.GetRequestIDFromContext(ctx)
	if err != nil {
		t.Errorf("Expected no error in call to GetContextWithRequestID when requestid is available in the metadata")
	}
	if requestID != "1238394" {
		t.Errorf("Expected requestID to be 1238394 but got: %s", requestID)
	}
}

func TestGetRequestIDFromContextWithNoRequestIDReturnsError(t *testing.T) {
	_, err := rpc.GetRequestIDFromContext(context.Background())
	if err == nil {
		t.Errorf("GetRequestIDFromContext should return error when no request id is available")
	}
}
