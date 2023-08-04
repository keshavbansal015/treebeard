package utils

import "hash/fnv"

type Hasher struct {
	KnownHashes map[string]uint32
}

// It calculates hash of a string.
// If the hash is already known it uses KnownHashes.
func (h *Hasher) Hash(s string) uint32 {
	if hash, exists := h.KnownHashes[s]; exists {
		return hash
	}
	hash := fnv.New32a()
	hash.Write([]byte(s))
	val := hash.Sum32()
	h.KnownHashes[s] = val
	return val
}
