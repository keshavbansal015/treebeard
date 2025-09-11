package client

import (
	"log" // Import the log package
	"testing"
	"time"
)

// The following code assumes a NewRateLimit and Acquire/Release
// implementation exists in the 'client' package. For demonstration,
// here is a simple stub to make this test runnable.
/*
type RateLimit struct {
	limit int
	tokens chan struct{}
}

func NewRateLimit(limit int) *RateLimit {
	tokens := make(chan struct{}, limit)
	for i := 0; i < limit; i++ {
		tokens <- struct{}{}
	}
	return &RateLimit{
		limit:  limit,
		tokens: tokens,
	}
}

func (r *RateLimit) Acquire() {
	<-r.tokens
}

func (r *RateLimit) Release() {
	r.tokens <- struct{}{}
}
*/

func TestAcquireBlocksOtherAcquiresIfReleaseNotCalled(t *testing.T) {
	log.Println("DEBUG: Starting TestAcquireBlocksOtherAcquiresIfReleaseNotCalled...")

	limit := 5
	log.Printf("DEBUG: Creating a rate limit with a capacity of %d.", limit)
	rateLimit := NewRateLimit(limit)

	log.Println("DEBUG: Acquiring the first 5 tokens to exhaust the limit.")
	rateLimit.Acquire()
	rateLimit.Acquire()
	rateLimit.Acquire()
	rateLimit.Acquire()
	rateLimit.Acquire()
	log.Println("DEBUG: All initial tokens acquired. The next acquire should block.")

	timeout := time.After(2 * time.Second)
	ch := make(chan bool)

	log.Println("DEBUG: Starting a goroutine to acquire the next token. This should block.")
	go func() {
		rateLimit.Acquire()
		log.Println("DEBUG: Goroutine successfully acquired a token. This indicates a failure.")
		ch <- true
	}()

	log.Println("DEBUG: Waiting for the goroutine to either return or for the test to time out.")
	select {
	case <-ch:
		log.Println("ERROR: Goroutine finished. Acquire did not block as expected.")
		t.Fatal("Acquire did not block")
	case <-timeout:
		log.Println("DEBUG: Timeout reached. The goroutine was successfully blocked. Test passed.")
	}

	log.Println("DEBUG: TestAcquireBlocksOtherAcquiresIfReleaseNotCalled completed.")
}

func TestReleaseAllowsBlockedClientsToContinue(t *testing.T) {
	log.Println("DEBUG: Starting TestReleaseAllowsBlockedClientsToContinue...")

	limit := 1
	log.Printf("DEBUG: Creating a rate limit with a capacity of %d.", limit)
	rateLimit := NewRateLimit(limit)
	timeout := time.After(1 * time.Second)
	responseChan := make(chan bool)

	log.Println("DEBUG: Starting a goroutine for the first acquire. This should succeed immediately.")
	go func() {
		rateLimit.Acquire()
		log.Println("DEBUG: First goroutine successfully acquired the token.")
		responseChan <- true
	}()

	log.Println("DEBUG: Waiting for the first acquire to complete.")
	select {
	case <-responseChan:
		log.Println("DEBUG: First acquire completed successfully.")
	case <-timeout:
		log.Println("ERROR: Timed out waiting for the first acquire to complete.")
		t.Fatal("Timed out waiting for response")
	}

	log.Println("DEBUG: Releasing the token.")
	rateLimit.Release()

	log.Println("DEBUG: Starting a second goroutine. This should acquire the now-released token.")
	go func() {
		rateLimit.Acquire()
		log.Println("DEBUG: Second goroutine successfully acquired the token.")
		responseChan <- true
	}()

	log.Println("DEBUG: Waiting for the second acquire to complete.")
	select {
	case <-responseChan:
		log.Println("DEBUG: Second acquire completed successfully. Test passed.")
	case <-timeout:
		log.Println("ERROR: Timed out waiting for the second acquire to complete.")
		t.Fatal("Timed out waiting for response")
	}

	log.Println("DEBUG: TestReleaseAllowsBlockedClientsToContinue completed.")
}
