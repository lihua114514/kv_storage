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
