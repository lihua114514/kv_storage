package index

import (
	"github.com/stretchr/testify/assert"
	"kv_storage/bitcask-go/data"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	b := NewBtree()
	bRes := b.Put([]byte("aaa"), &data.LogRecordPos{
		Fid:    100,
		Offset: 1,
	})
	assert.True(t, bRes)
}
func TestBtree_Get(t *testing.T) {
	btree := NewBtree()
	res := btree.Put([]byte("aaa"), &data.LogRecordPos{
		Fid:    100,
		Offset: 1,
	})
	assert.True(t, res)

	res1 := btree.Get([]byte("aaa"))
	assert.Equal(t, uint32(100), res1.Fid)
}
func TestBtree_Delete(t *testing.T) {
	btree := NewBtree()
	res := btree.Put([]byte("aaa"), &data.LogRecordPos{
		Fid:    100,
		Offset: 1,
	})
	assert.True(t, res)
	res2 := btree.Delete([]byte("aaa"))
	assert.True(t, res2)
}
func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	// 1.BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.BTree 有数据的情况
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向遍历的 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
