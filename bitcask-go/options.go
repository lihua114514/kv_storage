package bitcask_go

type Options struct {
	DirPath     string
	MaxFileSize uint32
	//每次写入是否持久化
	SyncWrite bool
}
