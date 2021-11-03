package cuckoo

import (
	"container/list"
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func newElement() *Bucket {
	b := &Bucket{
		node: &KVPair{
			key: byte(1),
			val: byte(2),
		},
		stash: list.New(),
	}
	b.stash.PushBack(&KVPair{
		key: byte(2),
		val: byte(2),
	})
	b.stash.PushBack(&KVPair{
		key: byte(3),
		val: byte(2),
	})
	b.stash.PushBack(&KVPair{
		key: byte(4),
		val: byte(2),
	})
	return b
}

func Test_deleteElement(t *testing.T) {
	b := newElement()
	deleteElement(b, byte(2))
	assert.Equal(t, b.stash.Front().Value.(*KVPair).key, byte(3))

	b = newElement()
	deleteElement(b, byte(1))
	assert.Equal(t, b.node, (*KVPair)(unsafe.Pointer(nil)))

	b = newElement()
	deleteElement(b, -1)
	assert.Equal(t, b.node.key, byte(1))
	assert.Equal(t, b.stash.Front().Value.(*KVPair).key, byte(2))
}

func Test_arrayAppend(t *testing.T) {
	cf := NewCuckooFilter(1, 0, [3]HashFunc{})
	cf.Filter[0] = &Bucket{node: &KVPair{
		key: byte(1),
		val: byte(2),
	}, stash: list.New()}
	ok := stashAppend(cf.Filter[0], []byte("a"), []byte("b"), cf)
	assert.True(t, ok)

	cf = NewCuckooFilter(1, 0, [3]HashFunc{})
	cf.Filter[0] = newElement()
	cf.Filter[0].stash.PushBack(byte(5))
	ok = stashAppend(cf.Filter[0], []byte("a"), []byte("b"), cf)
	assert.NotNil(t, ok)
}

func Test_convert(t *testing.T) {
	res1, ok := convert("a")
	assert.True(t, ok)
	fmt.Println(res1)

	res2, ok := convert([]byte{97})
	assert.True(t, ok)
	fmt.Println(res2)

	_, ok = convert(15)
	assert.True(t, ok)
}
