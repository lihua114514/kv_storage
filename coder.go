package golsm

import "fmt"

const (
	LOG_RECORD_TYPE_NOMAL byte = iota
	LOG_RECORD_TYPE_DLETED
)

func EncoderKey(key []byte) []byte {
	lenth := len(key) + 1
	EncodeKey := make([]byte, lenth)
	EncodeKey[0] = LOG_RECORD_TYPE_NOMAL
	copy(EncodeKey[1:], key)
	return EncodeKey
}
func EncodeDletedKey() []byte {
	EncodeKey := make([]byte, 1)
	EncodeKey[0] = LOG_RECORD_TYPE_DLETED
	return EncodeKey
}

// DecoderKey 解码键值，若返回的第二个参数是true则为已删除
func DecoderKey(buf []byte) []byte {
	if buf[0] == LOG_RECORD_TYPE_DLETED {
		return nil
	}
	fmt.Print("decode done")
	return buf[1:]
}
