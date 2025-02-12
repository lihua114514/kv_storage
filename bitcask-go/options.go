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
)

var DefaultOptions = Options{
	DirPath:     "/home/lihua/kv_storage", //该文件夹下的测试文件夹
	MaxFileSize: 128 * 1024 * 1024,        //一个文件最大为128MB
	SyncWrite:   true,                     //写入后立即持久化
	IndexType:   BTreeType,                //默认为B+树
}
