package rpc_test

import (
	"testing"

	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"google.golang.org/grpc/metadata"
)

func TestGetContextWithRequestIDHasUUIDInMetadata(t *testing.T) {
	ctx := rpc.GetContextWithRequestID()
	md, _ := metadata.FromOutgoingContext(ctx)
	if len(md["requestid"]) != 1 {
		t.Errorf("Expected a value in the requestid metadata for the request but got metadata: %v", md)
	}
}

func TestGetContextWithRequestIDWhenCalledMultipleTimesGeneratesDistinctRequestIDs(t *testing.T) {
	ctx1 := rpc.GetContextWithRequestID()
	ctx2 := rpc.GetContextWithRequestID()
	md1, _ := metadata.FromOutgoingContext(ctx1)
	md2, _ := metadata.FromOutgoingContext(ctx2)
	if md1["requestid"][0] == md2["requestid"][0] {
		t.Errorf("Expected different values for requestid for different contexts but got: %s", md1["requestid"][0])
	}
}
