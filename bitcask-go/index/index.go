package index

import (
	"github.com/google/btree"
	"kv_storage/bitcask-go/data"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (aItem *Item) Less(bItem btree.Item) bool {
	// 比较逻辑: 通过 key 来进行排序
	return string(aItem.key) < string(bItem.(*Item).key)
}
