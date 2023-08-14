package utils

func ConvertIntSliceToInt32Slice(intSlice []int) []int32 {
	int32Slice := make([]int32, len(intSlice))
	for _, v := range intSlice {
		int32Slice = append(int32Slice, int32(v))
	}
	return int32Slice
}

func ConvertInt32SliceToIntSlice(int32Slice []int32) []int {
	intSlice := make([]int, len(int32Slice))
	for _, v := range int32Slice {
		intSlice = append(intSlice, int(v))
	}
	return intSlice
}
