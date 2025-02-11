package data

import (
	"encoding/binary"
	"hash/crc32"
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

// EnLogRecord :对传入的logrecord记录进行编码，返回字节数组和数字
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size  |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EnLogRecord(logRecord *LogRecord) ([]byte, uint32) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, MaxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5
	// 5 字节之后，存储的是 key 和 value 的长度信息
	// 使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Val)))

	var size = index + len(logRecord.Key) + len(logRecord.Val)
	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Val)

	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, uint32(size)
}

func DecodeRecordHeader(bytes []byte) (*LogRecordHeader, int) {
	//检验传入字节数组的长度
	if len(bytes) < 4 {
		return nil, 0
	}
	header := &LogRecordHeader{
		crc:     binary.LittleEndian.Uint32(bytes[:4]),
		recType: bytes[4],
	}
	var index = 5
	keySize, n := binary.Varint(bytes[index:])
	header.keySize = uint32(keySize)
	index += n

	valSize, n := binary.Varint(bytes[index:])
	header.valSize = uint32(valSize)
	index += n

	return header, index
}

// 对读取的内容进行解码
func DeLogRecord(bytes []byte) (*LogRecord, uint32) {
	return nil, 0
}

// 获取CRC校验码
func GetLogRecordCrc(record *LogRecord, header []byte) uint32 {
	if record == nil {
		return 0
	}
	// 计算 header 的 CRC
	crc := crc32.ChecksumIEEE(header)

	//因为key和value是变长的，故而不能直接对整个byte数组产生crc
	// 更新 crc 值：先对 Key 和 Value 进行 CRC 更新
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Val)

	return crc
}
