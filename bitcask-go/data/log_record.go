package data

import (
	"encoding/binary"
)

type LogRecordType = byte

const (
	LOG_RECORD_TYPE_NOMAL LogRecordType = iota
	LOG_RECORD_TYPE_DLETED
)

// MaxLogRecordSize crc type keySize valueSize
// 4    1   5   5
const MaxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 4 + 1

type LogRecord struct {
	Key  []byte
	Val  []byte
	Type LogRecordType
}
type LogRecordPos struct {
	Fid    uint32
	Offset uint32
}

type LogRecordHeader struct {
	crc     uint32        //crc校验
	recType LogRecordType //记录类型
	keySize uint32        //键
	valSize uint32        //值的大小
}

// 进行编码返回字节数组和数字
func EnLogRecord(logrecord *LogRecord) ([]byte, uint32) {
	return nil, 0
}

func DecodeRecordHeader(bytes []byte) (*LogRecordHeader, int) {
	return nil, 0
}

// 对读取的内容进行解码
func DeLogRecord(bytes []byte) (*LogRecord, uint32) {
	return nil, 0
}

// 获取CRC校验码
func getLogRecordCrc(record *LogRecord, header []byte) uint32 {
	return 0
}
