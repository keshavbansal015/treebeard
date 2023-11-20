package utils

import (
	"testing"
)

func TestBinaryReverse(t *testing.T) {
	reverse := BinaryReverse(11, 4) // 1011 -> 1101
	if reverse != 13 {
		t.Errorf("expected 13, got %d", reverse)
	}
}
