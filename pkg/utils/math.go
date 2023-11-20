package utils

func BinaryReverse(x int, maxBits int) int {
	var result int
	for i := 0; i < maxBits; i++ {
		result = result<<1 + x&1
		x >>= 1
	}
	return result
}
