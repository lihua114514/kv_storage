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

type IndexType = int8

const (
	//B树的索引
	BTreeType IndexType = iota + 1

	//自适应基数树
	ART
)

// 用于初始化索引的方法
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTreeType:
		return NewBtree()
	case ART:
		//TODO
		return nil
	default:
		panic("unknown index type")
		return nil
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (aItem *Item) Less(bItem btree.Item) bool {
	// 比较逻辑: 通过 key 来进行排序
	return string(aItem.key) < string(bItem.(*Item).key)
}
