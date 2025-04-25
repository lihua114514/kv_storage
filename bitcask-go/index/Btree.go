package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/lihua114514/kv_storage/bitcask-go/data"

	"github.com/google/btree"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

type BtreeItem struct {
	//当前遍历的位置
	currentNum int
	//是否是反向遍历
	reverse bool
	//存储的索引值
	value []*Item
}

func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(20),
		lock: &sync.RWMutex{},
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	//Put insert in memory
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}
func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bt.lock.Lock()
	bti := bt.tree.Get(it)
	bt.lock.Unlock()
	if bti == nil {
		return nil
	}
	return bti.(*Item).pos

}
func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem != nil {
		return false
	}
	return oldItem != nil
}

func NewBtreeItem(tree *btree.BTree, reverse bool) *BtreeItem {
	var idx int = 0
	it := make([]*Item, tree.Len())
	if !reverse {
		tree.Ascend(func(i btree.Item) bool {
			it[idx] = i.(*Item)
			idx++
			return true
		})
	} else {
		tree.Descend(func(i btree.Item) bool {
			it[idx] = i.(*Item)
			idx++
			return true
		})
	}

	return &BtreeItem{
		currentNum: 0,
		reverse:    reverse,
		value:      it,
	}
}

func (Bit *BtreeItem) Seek(key []byte) {
	if Bit.reverse {
		Bit.currentNum = sort.Search(len(Bit.value), func(i int) bool {
			return bytes.Compare(Bit.value[i].key, key) <= 0
		})
	} else {
		Bit.currentNum = sort.Search(len(Bit.value), func(i int) bool {
			return bytes.Compare(Bit.value[i].key, key) >= 0
		})
	}
}

func (Bit *BtreeItem) Valid() bool {
	return Bit.currentNum < len(Bit.value)
}
func (Bit *BtreeItem) Rewind() {
	Bit.currentNum = 0
}
func (Bit *BtreeItem) Key() []byte {

	return Bit.value[Bit.currentNum].key
}
func (Bit *BtreeItem) Value() *data.LogRecordPos {
	return Bit.value[Bit.currentNum].pos
}
func (Bit *BtreeItem) Next() {
	Bit.currentNum++
}
func (Bit *BtreeItem) Close() {
	Bit.value = nil
}
func (bt *Btree) Iterator(reverse bool) Iterator {
	if bt == nil {
		bt = NewBtree()
	}
	return NewBtreeItem(bt.tree, reverse)
}
func (bt *Btree) Size() int {
	return bt.tree.Len()
}
