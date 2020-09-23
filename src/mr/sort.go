package mr

//IKeyValueArr represents array of keyvalue pairs, for sorting
type IKeyValueArr []KeyValue

// for sorting
func (arr IKeyValueArr) Len() int {
	return len(arr)
}

func (arr IKeyValueArr) Swap(a, b int) {
	arr[a], arr[b] = arr[b], arr[a]
}

func (arr IKeyValueArr) Less(a, b int) bool {
	return arr[a].Key < arr[b].Key
}
