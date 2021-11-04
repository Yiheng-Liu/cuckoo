// Copyright (c) 2014-2015 Utkan Güngördü <utkan@freeconsole.org>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cuckoo

import (
	"container/list"
	"fmt"
	"math"
	"math/rand"
)

type KVPair struct {
	Key []byte
	Val []byte
}

type Bucket struct {
	Node  *KVPair
	Stash *list.List
}

type CuckooFilter struct {
	Filter     map[uint32]*Bucket
	Seed       uint32
	cycleCount byte
	hasher     [3]HashFunc
}

// NewCuckooFilter creates a new cuckoo filter, if seed == 0, then will generate a new seed
// if you want to use your own hash function, you can pass it, otherwise it will use default functions
func NewCuckooFilter(dataSize int, seed uint32, hasher [3]HashFunc) *CuckooFilter {
	cuckoo := &CuckooFilter{
		Filter: make(map[uint32]*Bucket, int(math.Ceil(float64(dataSize)*growParameter))),
	}
	if seed != 0 {
		cuckoo.Seed = seed
	}
	if hasher[0] == nil {
		hasher[0] = murmur3_32
	}
	if hasher[1] == nil {
		hasher[1] = xx_32
	}
	if hasher[2] == nil {
		hasher[2] = mem_32
	}
	cuckoo.hasher = hasher
	return cuckoo
}

// ReSeed the rand generator, if seed == 0, then generate a random number instead
func (cf *CuckooFilter) ReSeed(seed uint32) {
	if seed != 0 {
		cf.Seed = seed
		return
	}
	cf.Seed = rand.Uint32()
}

// Insert data into Filter
// now only support insert []byte
func (cf *CuckooFilter) Insert(data []byte, value []byte) (uint32, bool) {
	cf.cycleCount += 1
	key, ok := convert(data)
	if !ok {
		return 0, false
	}
	if index, _, ok := cf.insert(data, value, key, cf.hasher[0]); ok {
		return index, true
	}
	if index, _, ok := cf.insert(data, value, key, cf.hasher[1]); ok {
		return index, true
	}
	if index, val, ok := cf.insert(data, value, key, cf.hasher[2]); ok {
		return index, true
	} else if cf.cycleCount < 3 {
		kicked := val.Node
		deleteElement(val, kicked.Key)
		val.Node = &KVPair{
			Key: data,
			Val: value,
		}
		return cf.Insert(kicked.Key, kicked.Val)
	}
	return 0, false
}

// Delete data from Filter
func (cf *CuckooFilter) Delete(data []byte) bool {
	key, ok := convert(data)
	if !ok {
		return false
	}
	if ok := cf.delete(data, key, murmur3_32); ok {
		return true
	} else if ok := cf.delete(data, key, xx_32); ok {
		return true
	} else {
		return cf.delete(data, key, mem_32)
	}
}

// SearchAll possible buckets given a certain data
func (cf *CuckooFilter) SearchAll(data interface{}) ([]uint32, bool) {
	key, ok := convert(data)
	if !ok {
		return nil, false
	}
	return []uint32{cf.hasher[0](key, cf.Seed), cf.hasher[1](key, cf.Seed), cf.hasher[2](key, cf.Seed)}, true
}

func (cf *CuckooFilter) Search(data []byte) (uint32, bool) {
	key, ok := convert(data)
	if !ok {
		return 0, ok
	}
	if loc, ok := cf.search(data, cf.hasher[0], key); ok {
		return loc, ok
	}
	if loc, ok := cf.search(data, cf.hasher[1], key); ok {
		return loc, ok
	}
	if loc, ok := cf.search(data, cf.hasher[2], key); ok {
		return loc, ok
	}
	return 0, false
}

func (cf *CuckooFilter) search(data []byte, hashFunc HashFunc, key uint32) (uint32, bool) {
	hashVal := hashFunc(key, cf.Seed)
	if val, ok := cf.Filter[hashVal]; ok {
		if val.Node != nil && isEqual(val.Node.Key, data) {
			return hashVal, true
		}
		for i := val.Stash.Front(); i != nil; i = i.Next() {
			if isEqual(i.Value.(*KVPair).Key, data) {
				return hashVal, true
			}
		}
	}
	return 0, false
}

func (cf *CuckooFilter) insert(data []byte, value []byte, key uint32, hasher HashFunc) (uint32, *Bucket, bool) {
	try := hasher(key, cf.Seed)
	if val, ok := cf.Filter[try]; !ok {
		cf.Filter[try] = &Bucket{
			Node: &KVPair{
				Key: data,
				Val: value,
			},
			Stash: list.New(),
		}
		cf.cycleCount = 0
		return try, val, true
	} else if val.Node == nil {
		val.Node = &KVPair{
			Key: data,
			Val: value,
		}
		cf.cycleCount = 0
		return try, val, true
	} else if cf.cycleCount == 3 {
		return try, val, stashAppend(val, data, value, cf)
	}
	return try, cf.Filter[try], false
}

func (cf *CuckooFilter) delete(data []byte, key uint32, hasher HashFunc) bool {
	input := hasher(key, cf.Seed)
	if val, ok := cf.Filter[input]; ok {
		deleteElement(val, data)
		if val.Node == nil && val.Stash.Front() == nil {
			delete(cf.Filter, input)
		}
		return true
	}
	return false
}

// fmt format
func (cf *CuckooFilter) String() string {
	if len(cf.Filter) == 0 {
		return "nil filter"
	}
	output := ""
	for k, v := range cf.Filter {
		output += fmt.Sprintf("--------[%v]--------\nnode=[%v]\n", k, v.Node)
		output += "Stash = ["
		for i := v.Stash.Front(); i != nil; i = i.Next() {
			output += fmt.Sprintf("%v ", i.Value)
		}
		output += "]\n"
	}
	return output
}
