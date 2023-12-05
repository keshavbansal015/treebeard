package client

import (
	"testing"
	"time"
)

func TestAcquireBlocksOtherAcquiresIfReleaseNotCalled(t *testing.T) {
	rateLimit := NewRateLimit(5)
	rateLimit.Acquire()
	rateLimit.Acquire()
	rateLimit.Acquire()
	rateLimit.Acquire()
	rateLimit.Acquire()
	timeout := time.After(2 * time.Second)
	ch := make(chan bool)
	go func() {
		rateLimit.Acquire()
		ch <- true
	}()
	select {
	case <-ch:
		t.Fatal("Acquire did not block")
	case <-timeout:
	}

}

func TestReleaseAllowsBlockedClientsToContinue(t *testing.T) {
	rateLimit := NewRateLimit(1)
	timeout := time.After(1 * time.Second)
	responseChan := make(chan bool)
	go func() {
		rateLimit.Acquire()
		responseChan <- true
	}()
	select {
	case <-responseChan:
	case <-timeout:
		t.Fatal("Timed out waiting for response")
	}
	rateLimit.Release()
	go func() {
		rateLimit.Acquire()
		responseChan <- true
	}()
	select {
	case <-responseChan:
	case <-timeout:
		t.Fatal("Timed out waiting for response")
	}
}
