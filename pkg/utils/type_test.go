package utils

import "testing"

func TestConvertIntSliceToInt32Slice(t *testing.T) {
	intSlice := []int{1, 2, 3, 4, 5}
	int32Slice := ConvertIntSliceToInt32Slice(intSlice)
	if len(int32Slice) != len(intSlice) {
		t.Errorf("ConvertIntSliceToInt32Slice: length mismatch")
	}
	for i, v := range intSlice {
		if int32(v) != int32Slice[i] {
			t.Errorf("ConvertIntSliceToInt32Slice: value mismatch expected %d got %d", v, int32Slice[i])
		}
	}
}

func TestConvertInt32SliceToIntSlice(t *testing.T) {
	int32Slice := []int32{1, 2, 3, 4, 5}
	intSlice := ConvertInt32SliceToIntSlice(int32Slice)
	if len(intSlice) != len(int32Slice) {
		t.Errorf("ConvertInt32SliceToIntSlice: length mismatch")
	}
	for i, v := range int32Slice {
		if int(v) != intSlice[i] {
			t.Errorf("ConvertInt32SliceToIntSlice: value mismatch expected %d got %d", v, intSlice[i])
		}
	}
}
