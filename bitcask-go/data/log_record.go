package data

type LogRecordType = byte

const (
	LOG_RECORD_TYPE_NOMAL LogRecordType = iota
	LOG_RECORD_TYPE_DLETED
)

type LogRecord struct {
	Key  []byte
	Val  []byte
	Type LogRecordType
}
type LogRecordPos struct {
	Fid    uint32
	Offset uint32
}
