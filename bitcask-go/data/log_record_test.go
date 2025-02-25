package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestDecodeRecordHeader(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := DecodeRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int(7), size1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LOG_RECORD_TYPE_NOMAL, h1.recType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := DecodeRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LOG_RECORD_TYPE_NOMAL, h2.recType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valSize)

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := DecodeRecordHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LOG_RECORD_TYPE_DLETED, h3.recType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valSize)
}
func TestEnLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("bitcask-go"),
		Type: LOG_RECORD_TYPE_NOMAL,
	}
	res1, n1 := EnLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, uint32(5))

	// value 为空的情况
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LOG_RECORD_TYPE_NOMAL,
	}
	res2, n2 := EnLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, uint32(5))

	// 对 Deleted 情况的测试
	rec3 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("bitcask-go"),
		Type: LOG_RECORD_TYPE_DLETED,
	}
	res3, n3 := EnLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, uint32(5))
}
func TestGetLogRecordCrc(t *testing.T) {
	rec1 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("bitcask-go"),
		Type: LOG_RECORD_TYPE_NOMAL,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := GetLogRecordCrc(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc1)

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LOG_RECORD_TYPE_NOMAL,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := GetLogRecordCrc(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	rec3 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("bitcask-go"),
		Type: LOG_RECORD_TYPE_DLETED,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := GetLogRecordCrc(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)
}
