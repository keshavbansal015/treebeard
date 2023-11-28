package client

// RateLimit is a simple rate limiter that allows a maximum of tokensLimit requests at a time.
type RateLimit struct {
	// The number of requests that can be made in the current period.
	tokensLimit     int
	availableTokens chan struct{}
}

func NewRateLimit(tokensLimit int) *RateLimit {
	return &RateLimit{
		tokensLimit:     tokensLimit,
		availableTokens: make(chan struct{}),
	}
}

// Clients should call Start() before making requests so that the rate limiter has tokens to give out.
func (r *RateLimit) Start() {
	for i := 0; i < r.tokensLimit; i++ {
		go func() { r.availableTokens <- struct{}{} }()
	}
}

// Clients should call Wait() before making requests to ensure that the rate limiter has tokens to give out.
func (r *RateLimit) Wait() {
	<-r.availableTokens
}

// Clients should call AddToken() after making requests to return a token to the rate limiter.
func (r *RateLimit) AddToken() {
	go func() { r.availableTokens <- struct{}{} }()
}
