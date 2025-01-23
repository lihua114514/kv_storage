package index

import (
	"github.com/google/btree"
	"kv_storage/bitcask-go/data"
	"sync"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
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
	if oldItem == nil {

		return false
	}

	return true
}
