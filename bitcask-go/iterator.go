package bitcask_go

import (
	"bytes"
	"kv_storage/bitcask-go/index"
)

type Iterator struct {
	IndexIterator index.Iterator
	Db            *DB
	Options       IteratorOptions
}

func (it *Iterator) Seek(key []byte) {
	it.IndexIterator.Seek(key)
}
func (it *Iterator) Valid() bool {
	return it.IndexIterator.Valid()
}
func (it *Iterator) Rewind() {
	it.IndexIterator.Rewind()
	it.skipToNext()
}
func (it *Iterator) key() []byte {
	return it.IndexIterator.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	LogRecordPos := it.IndexIterator.Value()

	it.Db.mu.Lock()
	defer it.Db.mu.Unlock()
	Val, err := it.Db.PosGet(LogRecordPos)

	if err != nil {
		return nil, err
	}
	return Val, err
}
func (it *Iterator) Next() {
	it.IndexIterator.Next()
	it.skipToNext()
}
func (it *Iterator) Close() {
	it.IndexIterator.Close()
}
func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	indexIter := db.indexer.Iterator(options.Reverse)
	return &Iterator{
		IndexIterator: indexIter,
		Db:            db,
		Options:       options,
	}
}
func (it *Iterator) skipToNext() {
	prefixLen := len(it.Options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.IndexIterator.Valid(); it.IndexIterator.Next() {
		key := it.IndexIterator.Key()
		if prefixLen <= len(key) && bytes.Equal(it.Options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
