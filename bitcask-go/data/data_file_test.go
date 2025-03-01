package data

import (
	"kv_storage/bitcask-go/fio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	Dir := os.TempDir()
	dataFile1, err := OpenDataFile(Dir, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("bbb"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("ccc"))
	assert.Nil(t, err)

	//t.Logf("dataFile1: %v", dataFile1)
}
func TestDataFile_Write(t *testing.T) {
	Dir := os.TempDir()
	dataFile1, err := OpenDataFile(Dir, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

}
func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 123, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}
func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 456, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}
func TestDataFile_Read(t *testing.T) {
	dataFile, err := OpenDataFile("/home/lihua/kv_storage/bitcask-go/test_data", 3511996, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	lr := &LogRecord{
		Key:  []byte("kay_fi"),
		Val:  []byte("val_fi"),
		Type: LOG_RECORD_TYPE_NOMAL,
	}
	kvbuf, size1 := EnLogRecord(lr)
	err = dataFile.Write(kvbuf)
	assert.Nil(t, err)

	logr, offset, _ := dataFile.ReadDataFile(0)

	assert.Equal(t, offset, size1)
	assert.Equal(t, lr, logr)

	// 多条 LogRecord，从不同的位置读取
	rec2 := &LogRecord{
		Key: []byte("name"),
		Val: []byte("a new value"),
	}
	res2, size2 := EnLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadDataFile(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:  []byte("1"),
		Val:  []byte(""),
		Type: LOG_RECORD_TYPE_DLETED,
	}
	res3, size3 := EnLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)

	readRec3, readSize3, err := dataFile.ReadDataFile(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)

}
