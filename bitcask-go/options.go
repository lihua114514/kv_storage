package bitcask_go

type Options struct {
	//文件路径
	DirPath string
	//最大文件大小
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

	//SkipList
	SKIPLIST
)

var DefaultOptions = Options{
	DirPath:     "/home/lihua/kv_storage", //该文件夹下的测试文件夹
	MaxFileSize: 128 * 1024 * 1024,        //一个文件最大为128MB
	SyncWrite:   true,                     //写入后立即持久化
	IndexType:   SKIPLIST,                 //默认为B+树
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

var DefaultIteratorOption = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 提交时是否 sync 持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
