package cuckoo

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

const bytes = 3

// basic test
func TestCuckooFilter(t *testing.T) {
	// new cuckoo filter
	cf := NewCuckooFilter(3, 0, [3]HashFunc{})
	fmt.Println(cf)

	data := []byte("abc")
	val := []byte("Val")
	// insert an element
	index, ok := cf.Insert(data, val)
	assert.True(t, ok)
	fmt.Println(index)
	fmt.Println(cf)

	// search an existing element
	loc, ok := cf.Search(data)
	assert.True(t, ok)
	fmt.Println(loc)

	// delete an element
	ok = cf.Delete(data)
	assert.True(t, ok)
	fmt.Println(cf)

	// search a not existing element
	loc, ok = cf.Search(data)
	assert.False(t, ok)
	fmt.Println(loc)

	// reinsert the same element
	index, ok = cf.Insert([]byte("abc"), []byte("Val"))
	assert.True(t, ok)
	fmt.Println(index)
	fmt.Println(cf)

	// search all possible locations of an element
	res, ok := cf.SearchAll(data)
	assert.True(t, ok)
	fmt.Println(res)
}

func TestScaleData(t *testing.T) {
	size := 1000
	dataset := generateBytes(size)
	val := generateBytes(size)
	cf := NewCuckooFilter(size, 0, [3]HashFunc{})
	// test insert
	for i := 0; i < len(dataset); i++ {
		_, ok := cf.Insert(dataset[i], val[i])
		assert.True(t, ok)
	}
	// test delete
	for i := 0; i < len(dataset); i++ {
		ok := cf.Delete(dataset[i])
		assert.True(t, ok)
	}
}

func generateBytes(size int) [][]byte {
	res := make([][]byte, size)
	for i := 0; i < size; i++ {
		res[i] = make([]byte, bytes)
		rand.Read(res[i])
	}
	return res
}

func mockHasher1(key, seed uint32) uint32 {
	return 1
}

func mockHasher2(key, seed uint32) uint32 {
	return 2
}

func mockHasher3(key, seed uint32) uint32 {
	return 3
}

func TestInsertWithCollision(t *testing.T) {
	size := 5
	dataset := generateBytes(size)
	cf := NewCuckooFilter(size, 0, [3]HashFunc{mockHasher1, mockHasher2, mockHasher3})
	for i := 0; i < len(dataset); i++ {
		index, ok := cf.Insert(dataset[i], []byte("Val"))
		assert.True(t, ok)
		fmt.Println(index)
		fmt.Println(cf)
	}
}
