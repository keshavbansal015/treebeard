package client

import (
	"testing"
	"time"
)

func TestRateLimitAddsTokensAfterCallingStart(t *testing.T) {
	rateLimit := NewRateLimit(10)
	rateLimit.Start()
	timeout := time.After(1 * time.Second)
	responseChan := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			rateLimit.Wait()
			responseChan <- true
		}()
	}
	for i := 0; i < 10; i++ {
		select {
		case <-responseChan:
		case <-timeout:
			t.Fatal("Timed out waiting for response")
		}
	}
}

func TestAddTokenAllowsBlockedClientsToContinue(t *testing.T) {
	rateLimit := NewRateLimit(1)
	rateLimit.Start()
	timeout := time.After(1 * time.Second)
	responseChan := make(chan bool)
	go func() {
		rateLimit.Wait()
		responseChan <- true
	}()
	select {
	case <-responseChan:
	case <-timeout:
		t.Fatal("Timed out waiting for response")
	}
	rateLimit.AddToken()
	go func() {
		rateLimit.Wait()
		responseChan <- true
	}()
	select {
	case <-responseChan:
	case <-timeout:
		t.Fatal("Timed out waiting for response")
	}
}
