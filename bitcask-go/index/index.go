package index

import (
	"kv_storage/bitcask-go/data"

	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	Iterator(reverse bool) Iterator
	// Size 索引中的数据量
	Size() int
}

type IndexType = int8

const (
	//B树的索引
	BTreeType IndexType = iota + 1

	//LSM树
	LSM
	//跳表
	SKIPLIST
)

// 用于初始化索引的方法
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTreeType:
		return NewBtree()
	case LSM:
		//TODO
		return nil
	case SKIPLIST:
		return NewSkipList()
	default:
		panic("unknown index type")

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

// 用于便利索引的接口
type Iterator interface {
	Seek(key []byte)
	Valid() bool
	Rewind()
	Key() []byte
	Value() *data.LogRecordPos
	Next()
	Close()
}
