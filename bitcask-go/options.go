package bitcask_go

type Options struct {
	DirPath     string
	MaxFileSize uint32
	//每次写入是否持久化
	SyncWrite bool
	//内存索引类型
	IndexType IndexType
}

type IndexType = int8

const (
	//B树的索引
	BTreeType IndexType = iota + 1

	//自适应基数树
	ART
)
