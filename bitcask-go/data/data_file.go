package data

import "kv_storage/bitcask-go/fio"

type DataFile struct {
	FileId      uint32       //文件的ID
	WriteOffset uint32       //偏移
	IoManger    fio.IoManger //用于文件IO
}

func OpenDataFile(DirPath string, FileID uint32) (*DataFile, error) {
	//TODO
	return nil, nil
}

func (df *DataFile) ReadDataFile(offset uint32) (*LogRecord, error) {
	return nil, nil
}

// 进行编码返回字节数组和数字
func EnLogRecord(logrecord *LogRecord) ([]byte, uint32) {
	return nil, 0
}

func (df *DataFile) Write(buffer []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
