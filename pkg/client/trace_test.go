package client

import (
	"testing"
)

func TestPadBlockValueMakesBlocksOfCorrectSize(t *testing.T) {
	blockSizeBytes := 10
	blockValue := "1234567"
	paddedBlockValue := padBlockValue(blockValue, blockSizeBytes)
	if len(paddedBlockValue) != blockSizeBytes {
		t.Errorf("padded block value is not of the correct size")
	}
}
