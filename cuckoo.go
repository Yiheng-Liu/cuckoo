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
	Val [2][]byte
}

type Bucket struct {
	Node  *KVPair
	Stash *list.List
}

type CuckooFilter struct {
	Filter     []*Bucket
	Seed       uint32
	cycleCount byte
	hasher     [3]HashFunc
	size       int
}

// NewCuckooFilter creates a new cuckoo filter, if seed == 0, then will generate a new seed
// if you want to use your own hash function, you can pass it, otherwise it will use default functions
func NewCuckooFilter(dataSize int, seed uint32, hasher [3]HashFunc) *CuckooFilter {
	size := int(math.Ceil(float64(dataSize) * growParameter))
	cuckoo := &CuckooFilter{
		Filter: make([]*Bucket, size),
		size:   size,
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
func (cf *CuckooFilter) Insert(data []byte, value [2][]byte) bool {
	cf.cycleCount += 1
	key, ok := convert(data)
	if !ok {
		return false
	}
	if _, ok := cf.insert(data, value, key, cf.hasher[0]); ok {
		return true
	}
	if _, ok := cf.insert(data, value, key, cf.hasher[1]); ok {
		return true
	}
	if val, ok := cf.insert(data, value, key, cf.hasher[2]); ok {
		return true
	} else if cf.cycleCount < 3 {
		kicked := val.Node
		deleteElement(val, kicked)
		val.Node = &KVPair{
			Key: data,
			Val: value,
		}
		return cf.Insert(kicked.Key, kicked.Val)
	}
	return false
}

// Delete data from Filter
func (cf *CuckooFilter) Delete(data interface{}) bool {
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
	return []uint32{murmur3_32(key, cf.Seed) % uint32(cf.size), xx_32(key, cf.Seed) % uint32(cf.size), mem_32(key, cf.Seed) % uint32(cf.size)}, true
}

func (cf *CuckooFilter) insert(data []byte, value [2][]byte, key uint32, hasher HashFunc) (*Bucket, bool) {
	try := hasher(key, cf.Seed) % uint32(cf.size)
	if val := cf.Filter[try]; val == nil {
		cf.Filter[try] = &Bucket{
			Node: &KVPair{
				Key: data,
				Val: value,
			},
			Stash: list.New(),
		}
		cf.cycleCount = 0
		return val, true
	} else if val.Node == nil {
		val.Node = &KVPair{
			Key: data,
			Val: value,
		}
		cf.cycleCount = 0
		return val, true
	} else if cf.cycleCount == 3 {
		return val, stashAppend(val, data, value, cf)
	}
	return cf.Filter[try], false
}

func (cf *CuckooFilter) delete(data interface{}, key uint32, hasher HashFunc) bool {
	input := hasher(key, cf.Seed) % uint32(cf.size)
	if val := cf.Filter[input]; val != nil {
		deleteElement(val, data)
		return true
	}
	return false
}

func (cf *CuckooFilter) StashUsed() int {
	count := 0
	for _, bucket := range cf.Filter {
		if bucket != nil && bucket.Stash != nil {
			count += bucket.Stash.Len()
		}
	}
	return count
}

func (cf *CuckooFilter) LargestStash() int {
	max := -1
	for _, bucket := range cf.Filter {
		if bucket != nil && bucket.Stash != nil {
			if max < bucket.Stash.Len() {
				max = bucket.Stash.Len()
			}
		}
	}
	return max
}

func (cf *CuckooFilter) LoadFactor() float64 {
	totalUsed := 0
	for _, bucket := range cf.Filter {
		if bucket != nil {
			totalUsed++
		}
	}
	return float64(totalUsed) / float64(cf.size)
}

// fmt format
func (cf *CuckooFilter) String() string {
	if len(cf.Filter) == 0 {
		return "nil filter"
	}
	output := ""
	for k, v := range cf.Filter {
		if v == nil {
			continue
		}
		output += fmt.Sprintf("--------[%v]--------\nnode=[%v]\n", k, v.Node)
		output += "Stash = ["
		for i := v.Stash.Front(); i != nil; i = i.Next() {
			output += fmt.Sprintf("%v ", i.Value)
		}
		output += "]\n"
	}
	return output
}
