package fio

import "errors"

type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Close() error //持久化数据到磁盘
	Size() (int64, error)
	Sync() error
}
type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

// NewIOManager 初始化 IOManager，目前只支持标准 FileIO
func NewIOManager(fileName string, IoType FileIOType) (IOManager, error) {
	switch IoType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		return nil, errors.New("not impliment")
	}

}
