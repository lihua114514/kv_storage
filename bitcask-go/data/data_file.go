package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"kv_storage/bitcask-go/fio"
	"path/filepath"
)

const DataFilesuffix = ".data"

type DataFile struct {
	FileId      uint32        //文件的ID
	WriteOffset uint32        //偏移
	IoManger    fio.IOManager //用于文件IO
}

func OpenDataFile(DirPath string, FileID uint32) (*DataFile, error) {
	//打开数据文件
	filename := filepath.Join(DirPath, fmt.Sprintf("%09d", FileID)+DataFilesuffix)
	//os.MkdirAll(DirPath, 0744)
	IoManger, err := fio.NewIOManager(filename)
	if err != nil {
		return nil, err
	}
	DataFile := &DataFile{
		FileId:      FileID,
		WriteOffset: 0,
		IoManger:    IoManger,
	}
	return DataFile, nil
}

func (df *DataFile) ReadDataFile(offset uint32) (*LogRecord, uint32, error) {
	size, err := df.IoManger.Size()
	if err != nil {
		return nil, 0, err
	}

	//如果读取的文件长度已经超过了文件最大长度，只需读取到文件末尾即可
	var headers int64 = MaxLogRecordHeaderSize
	if int64(offset)+headers > size {
		headers = size + int64(offset)
	}

	HeaderBuffer, err := df.ReadNBytes(uint32(headers), int64(offset))
	if err != nil {
		return nil, 0, err
	}
	header, headSize := DecodeRecordHeader(HeaderBuffer)

	//检测是否为空
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valSize == 0 {
		return nil, 0, io.EOF
	}

	keySize := int(header.keySize)
	valSize := int(header.valSize)
	//正式读取数值
	logRecord := &LogRecord{Type: header.recType}
	var recordSize = header.keySize + header.valSize + uint32(headSize)
	if keySize > 0 || valSize > 0 {
		kvBuf, err := df.ReadNBytes(uint32(keySize+valSize), int64(int(offset)+(headSize)))
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Val = kvBuf[keySize:]
	}

	//进行CRC检测
	var crcRead_ = GetLogRecordCrc(logRecord, HeaderBuffer[crc32.Size:headSize])
	if crcRead_ != header.crc {
		return nil, 0, errors.New("invalid crc")
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buffer []byte) error {
	bits, err := df.IoManger.Write(buffer)
	if err != nil {
		return err
	}
	df.WriteOffset += uint32(bits)
	return err
}

func (df *DataFile) Sync() error {
	return df.IoManger.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManger.Close()
}

func (df *DataFile) ReadNBytes(n uint32, offset int64) ([]byte, error) {
	//offset是读取开始的位置
	buffer := make([]byte, n)
	df.IoManger.Read(buffer, offset)
	return buffer, nil
}
