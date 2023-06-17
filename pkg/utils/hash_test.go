package utils_test

import (
	"testing"

	utils "github.com/dsg-uwaterloo/oblishard/pkg/utils"
)

type hashTest struct {
	input        string
	expectedHash uint32
}

var hashTestCases = []hashTest{
	{"a", 3826002220},
	{"b", 3876335077},
	{"c", 3859557458},
	{"d", 3775669363},
	{"1", 873244444},
	{"2", 923577301},
	{"3", 906799682},
	{"4", 822911587},
	{"a", 3826002220},
}

func TestHash(t *testing.T) {
	hasher := utils.Hasher{KnownHashes: make(map[string]uint32)}
	for _, test := range hashTestCases {
		output := hasher.Hash(test.input)
		if output != test.expectedHash {
			t.Errorf("Hash output %d is not equal to the expected hash output %d",
				output, test.expectedHash)
		}
	}
}
