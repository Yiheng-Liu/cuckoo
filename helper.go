package cuckoo

import (
	"math/big"
	"reflect"
	"unsafe"
)

//
func isEqual(first []byte, second []byte) bool {
	if len(first) != len(second) {
		return false
	}
	for i := 0; i < len(first); i++ {
		if first[i] != second[i] {
			return false
		}
	}
	return true
}

// deleteElement deletes the element with given location from Bucket
func deleteElement(val *Bucket, data []byte) {
	if isEqual(val.Node.Key, data) {
		val.Node = nil
		return
	}
	for it := val.Stash.Front(); it != val.Stash.Back(); it = it.Next() {
		if isEqual(it.Value.(*KVPair).Key, data) {
			val.Stash.Remove(it)
			return
		}
	}
}

// stashAppend appends a data to a Bucket
func stashAppend(val *Bucket, data []byte, value []byte, cf *CuckooFilter) bool {
	val.Stash.PushBack(&KVPair{
		Key: data,
		Val: value,
	})
	cf.cycleCount = 0
	return true
}

// convert data to uint32, if data is not a num, string or []byte will return error
func convert(data interface{}) (uint32, bool) {
	switch reflect.TypeOf(data).Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return *(*uint32)(unsafe.Pointer(&data)), true
	case reflect.String, reflect.Slice:
		var d []byte
		if s, ok := data.(string); ok {
			d = []byte(s)
		} else {
			d, ok = data.([]byte)
			if !ok {
				return 0, false
			}
		}
		key := uint32(new(big.Int).SetBytes(d).Uint64())
		return key, true
	}
	return 0, false
}
