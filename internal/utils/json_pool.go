package utils

import (
	"sync"
)

// Pool for transfer data maps to avoid allocations
var transferDataPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 20) // Pre-allocate reasonable capacity
	},
}



// GetTransferDataMap gets a pooled map for transfer data
func GetTransferDataMap() map[string]interface{} {
	m := transferDataPool.Get().(map[string]interface{})
	// Clear the map but keep capacity
	for k := range m {
		delete(m, k)
	}
	return m
}

// PutTransferDataMap returns a transfer data map to the pool
func PutTransferDataMap(m map[string]interface{}) {
	if m != nil {
		transferDataPool.Put(m)
	}
}

 