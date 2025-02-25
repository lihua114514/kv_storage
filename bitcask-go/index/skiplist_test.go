package index

import (
	"kv_storage/bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkip_Put(t *testing.T) {
	skip := NewSkipList()
	res1 := skip.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, res1)

	res2 := skip.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, res2)

	res3 := skip.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, res3)
}

func TestSkip_Get(t *testing.T) {
	b := NewSkipList()
	bRes := b.Put([]byte("aaa"), &data.LogRecordPos{
		Fid:    100,
		Offset: 1,
	})
	assert.True(t, bRes)
	skip := NewSkipList()
	res1 := skip.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, res1)
	res2 := skip.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, res2)
	res3 := skip.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.True(t, res3)
	res4 := skip.Get([]byte("key-1"))
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 12}, res4)

}

func TestSkip_Delete(t *testing.T) {
	skip := NewSkipList()
	res := skip.Put([]byte("aaa"), &data.LogRecordPos{
		Fid:    100,
		Offset: 1,
	})
	it := skip.NewIterator(true)
	assert.NotNil(t, it.skiplist)
	assert.NotNil(t, it.current)
	assert.True(t, res)
	res2 := skip.Delete([]byte("aaa"))
	assert.NotNil(t, res2)
}
func TestSKip_Iterator(t *testing.T) {
	skip := NewSkipList()
	// 1.SkipList 为空的情况
	iter1 := skip.NewIterator(true)
	assert.Equal(t, false, iter1.Valid())

	//	2.SkipList 有数据的情况
	skip.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := skip.NewIterator(true)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	skip.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	skip.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	skip.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := skip.NewIterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	// 3.有多条数据
	skip.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	skip.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	skip.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter4 := skip.NewIterator(true)
	for iter4.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter5 := skip.NewIterator(true)
	for iter5.Rewind(); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter6 := skip.NewIterator(true)
	for iter6.Seek([]byte("cc")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter5.Key())
	}

}
