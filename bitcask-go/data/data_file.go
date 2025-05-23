package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"path/filepath"

	"github.com/lihua114514/kv_storage/bitcask-go/fio"
)

const (
	DataFilesuffix        = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

type DataFile struct {
	FileId      uint32        //文件的ID
	WriteOffset uint32        //偏移
	IoManger    fio.IOManager //用于文件IO
}

func OpenDataFile(DirPath string, FileID uint32, IoType fio.FileIOType) (*DataFile, error) {
	//打开数据文件
	filename := filepath.Join(DirPath, fmt.Sprintf("%09d", FileID)+DataFilesuffix)

	return newDataFile(filename, FileID, IoType)
}
func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	//启动时使用MMap方式或者标准IO模式进行IO
	IoManger, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	DataFile := &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IoManger:    IoManger,
	}
	return DataFile, nil
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFilesuffix)
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
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key: key,
		Val: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EnLogRecord(record)
	return df.Write(encRecord)
}
