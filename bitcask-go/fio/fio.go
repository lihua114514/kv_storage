package fio

const DataFilePerm = 644

type IoManger interface {
	Read([]byte, int64) (int64, error)
	Write([]byte) (int64, error)
	Close() error //持久化数据到磁盘
	Sync() error
}
