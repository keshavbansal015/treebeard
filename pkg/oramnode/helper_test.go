package oramnode

import "testing"

func TestDistributeBucketIDs(t *testing.T) {
	bucketIDs := []int{1, 3, 4, 5, 7, 8, 9, 10}
	maxElementCount := 3
	batches := distributeBucketIDs(bucketIDs, maxElementCount)
	elementsFound := []int{}
	for _, batch := range batches {
		if len(batch) > maxElementCount {
			t.Errorf("expected batch size %d, but got %d", maxElementCount, len(batch))
		}
		elementsFound = append(elementsFound, batch...)
	}
	expectedFound := map[int]bool{
		1:  true,
		3:  true,
		4:  true,
		5:  true,
		7:  true,
		8:  true,
		9:  true,
		10: true,
	}
	if len(elementsFound) != len(expectedFound) {
		t.Errorf("expected %d elements, but got %d", len(expectedFound), len(elementsFound))
	}
	for _, element := range elementsFound {
		if _, exists := expectedFound[element]; !exists {
			t.Errorf("expected element %d to exist", element)
		}
	}
}
