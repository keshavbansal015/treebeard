package oramnode

func distributeBucketIDs(bucketIDs []int, maxElementCount int) [][]int {
	var bucketIDBatches [][]int
	for i := 0; i < len(bucketIDs); i += maxElementCount {
		end := i + maxElementCount
		if end > len(bucketIDs) {
			end = len(bucketIDs)
		}
		bucketIDBatches = append(bucketIDBatches, bucketIDs[i:end])
	}
	return bucketIDBatches
}
