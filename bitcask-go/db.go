package bitcask_go

import (
	"errors"
	"kv_storage/bitcask-go/data"
	"kv_storage/bitcask-go/index"
	"os"
	"sync"
)

type DB struct {
	option     Options
	mu         *sync.RWMutex
	activeFile *data.DataFile
	oldFiles   map[uint32]*data.DataFile
	indexer    index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return index.KeyIsEmpty
	}

	//构造LogRecord结构体
	LogRecord_ := &data.LogRecord{
		Key:  key,
		Val:  value,
		Type: data.LOG_RECORD_TYPE_NOMAL,
	}

	pos, err := db.AppendLogRecord(LogRecord_)
	if err != nil {
		return err
	}
	//存入内存索引
	if ok := db.indexer.Put(key, pos); !ok {
		return index.IndexUpdateFail
	}

	return nil
}
func checkOption(option Options) error {
	if option.DirPath == "" {
		return errors.New("dirPath is empty")
	}
	if option.MaxFileSize <= 0 {
		return errors.New("max file size is zero")
	}
	return nil
}
func Open(option Options) (*DB, error) {
	//检查配置是否有问题
	if err := checkOption(option); err != nil {
		return nil, err
	}
	//检验传入的数据目录是否存在，若不存在，生成文件目录
	if _, err := os.Stat(option.DirPath); err != nil {
		return nil, err
	}
	//初始化数据库实例
	db := &DB{
		option:   option,
		mu:       new(sync.RWMutex),
		oldFiles: make(map[uint32]*data.DataFile),
		indexer:  index.NewIndexer(index.IndexType(option.IndexType)),
	}
	//加载数据文件

	return db, nil
}
func (db *DB) AppendLogRecord(LogRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//判断当前活跃文件是否存在，因为数据库在写入的时候是没有文件生成的
	//若不存在则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	//写入数字编码
	EncRecord, size := data.EnLogRecord(LogRecord)
	if db.activeFile.WriteOffset+size > db.option.MaxFileSize {
		//持久化数据到磁盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//将该文件转换为旧的数据文件
		db.oldFiles[db.activeFile.FileId] = db.activeFile
		//打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	offs := db.activeFile.WriteOffset
	if err := db.activeFile.Write(EncRecord); err != nil {
		return nil, err
	}
	if db.option.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: offs,
	}
	return pos, nil
}
func (db *DB) setActiveFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	datafile, err := data.OpenDataFile(db.option.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = datafile
	return nil
}
func (db *DB) Get(key []byte) ([]byte, error) {

	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, index.KeyIsEmpty
	}
	logRecordPos := db.indexer.Get(key)
	if logRecordPos == nil {
		return nil, index.IndexNotFound
	}
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, index.DataFileNotExists
	}
	//根据偏移量读取数据
	buffer, err := dataFile.ReadDataFile(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if buffer.Type == data.LOG_RECORD_TYPE_DLETED {
		return nil, index.KeyNotExists
	}
	return buffer.Val, nil
}
func (db *DB) Delete(key []byte) bool {
	return false
}
