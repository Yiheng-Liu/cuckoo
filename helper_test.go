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
		Node: &KVPair{
			Key: byte(1),
			Val: byte(2),
		},
		Stash: list.New(),
	}
	b.Stash.PushBack(&KVPair{
		Key: byte(2),
		Val: byte(2),
	})
	b.Stash.PushBack(&KVPair{
		Key: byte(3),
		Val: byte(2),
	})
	b.Stash.PushBack(&KVPair{
		Key: byte(4),
		Val: byte(2),
	})
	return b
}

func Test_deleteElement(t *testing.T) {
	b := newElement()
	deleteElement(b, byte(2))
	assert.Equal(t, b.Stash.Front().Value.(*KVPair).Key, byte(3))

	b = newElement()
	deleteElement(b, byte(1))
	assert.Equal(t, b.Node, (*KVPair)(unsafe.Pointer(nil)))

	b = newElement()
	deleteElement(b, -1)
	assert.Equal(t, b.Node.Key, byte(1))
	assert.Equal(t, b.Stash.Front().Value.(*KVPair).Key, byte(2))
}

func Test_arrayAppend(t *testing.T) {
	cf := NewCuckooFilter(1, 0, [3]HashFunc{})
	cf.Filter[0] = &Bucket{Node: &KVPair{
		Key: byte(1),
		Val: byte(2),
	}, Stash: list.New()}
	ok := stashAppend(cf.Filter[0], []byte("a"), []byte("b"), cf)
	assert.True(t, ok)

	cf = NewCuckooFilter(1, 0, [3]HashFunc{})
	cf.Filter[0] = newElement()
	cf.Filter[0].Stash.PushBack(byte(5))
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
