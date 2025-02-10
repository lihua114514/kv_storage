package fio

const DataFilePerm = 0744

type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Close() error //持久化数据到磁盘
	Size() (int64, error)
	Sync() error
}

// NewIOManager 初始化 IOManager，目前只支持标准 FileIO
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)

}
