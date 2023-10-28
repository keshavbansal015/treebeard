package utils

import (
	"hash/fnv"
	"sync"
)

// Hahsher is thread-safe.
// If you see contention on the mutex,
// you may want to consider a sync.Map.
// Since we only write to the map once per key,
// and read from it many times, it might make sense.
type Hasher struct {
	KnownHashes map[string]uint32
	mu          sync.Mutex
}

// It calculates hash of a string.
// If the hash is already known it uses KnownHashes.
func (h *Hasher) Hash(s string) uint32 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hash, exists := h.KnownHashes[s]; exists {
		return hash
	}
	hash := fnv.New32a()
	hash.Write([]byte(s))
	val := hash.Sum32()
	h.KnownHashes[s] = val
	return val
}
