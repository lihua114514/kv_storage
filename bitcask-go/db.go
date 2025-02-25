package bitcask_go

import (
	"errors"
	"io"
	"kv_storage/bitcask-go/data"
	"kv_storage/bitcask-go/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	option     Options
	mu         *sync.RWMutex  //互斥锁
	seqNo      uint64         // 事务序列号，全局递增
	activeFile *data.DataFile //活跃文件
	fileIDs    []int          //目录下的数据文件，只能在加载数据的时候使用
	oldFiles   map[uint32]*data.DataFile
	indexer    index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return KeyIsEmpty
	}

	//构造LogRecord结构体
	LogRecord_ := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Val:  value,
		Type: data.LOG_RECORD_TYPE_NOMAL,
	}

	pos, err := db.AppendLogRecordWithLock(LogRecord_)
	if err != nil {
		return err
	}
	//存入内存索引
	if ok := db.indexer.Put(key, pos); !ok {
		return IndexUpdateFail
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
		indexer:  index.NewIndexer(option.IndexType),
	}
	//加载数据文件
	if err := db.loadDatafile(); err != nil {
		return nil, err
	}

	//从数据文件中加载索引
	if err := db.loadIndexDatafile(); err != nil {
		return nil, err
	}
	return db, nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDatafile() error {
	files, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}
	//存放文件ID的数组
	var FileIDs []int
	//寻找以.data为结尾的文件
	for _, file := range files {
		if strings.HasSuffix(file.Name(), data.DataFilesuffix) {
			FileName := strings.Split(file.Name(), ".")
			FileId, err := strconv.Atoi(FileName[0])
			if err != nil {
				return DataFileERROR
			}
			FileIDs = append(FileIDs, FileId)
		}

	}
	//对数据文件进行排序
	sort.Ints(FileIDs)
	db.fileIDs = FileIDs

	//遍历每个文件
	for i, FileID := range FileIDs {
		datafile, err := data.OpenDataFile(db.option.DirPath, uint32(FileID))
		if err != nil {
			return err
		}
		//设置最后一个文件为活跃文件
		if i == len(FileIDs)-1 {
			//是最后一个文件
			db.activeFile = datafile
		} else {
			//不是最后一个文件设置将文件设置为老文件
			db.oldFiles[uint32(i)] = datafile
		}

	}
	return nil
}

// 从数据文件中加载索引
// 便利索所有数据文件并加载到内存中
func (db *DB) loadIndexDatafile() error {
	if len(db.fileIDs) == 0 {
		//该数据库为空不做处理
		return nil
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool = true
		if typ == data.LOG_RECORD_TYPE_DLETED {
			ok = db.indexer.Delete(key)
			if !ok {
				panic("fail to update index in memory")
			}
		} else {
			ok = db.indexer.Put(key, pos)
			if !ok {
				panic("fail to update index in memory")
			}
		}
		if !ok {
			panic("fail to update index in memory")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	for i, fileID := range db.fileIDs {
		var datafile *data.DataFile
		var fid = uint32(fileID)
		//将datafile 取出
		if fid == db.activeFile.FileId {
			datafile = db.activeFile
		} else {
			datafile = db.oldFiles[fid]
		}
		//将读取的文件中的信息传入内存索引
		var offset uint32 = 0
		for {
			LogRecord, size, err := datafile.ReadDataFile(offset)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			LogRecord_ := &data.LogRecordPos{
				Fid:    fid,
				Offset: offset,
			}
			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(LogRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(LogRecord.Key, LogRecord.Type, LogRecord_)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				if LogRecord.Type == data.LOG_RECORD_TXN_FINISHED {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					LogRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: LogRecord,
						Pos:    LogRecord_,
					})
				}
			}
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			offset += size
		}
		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOffset = offset
		}

	}
	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) AppendLogRecordWithLock(LogRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.AppendLogRecord(LogRecord)
}
func (db *DB) AppendLogRecord(LogRecord *data.LogRecord) (*data.LogRecordPos, error) {

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
		return nil, KeyIsEmpty
	}
	logRecordPos := db.indexer.Get(key)
	if logRecordPos == nil {
		return nil, IndexNotFound
	}

	return db.PosGet(logRecordPos)
}

func (db *DB) PosGet(logRecordPos *data.LogRecordPos) ([]byte, error) {
	//根据位置信息获取内容
	//调用该函数时需要加锁
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, DataFileNotExists
	}
	//根据偏移量读取数据
	buffer, _, err := dataFile.ReadDataFile(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if buffer.Type == data.LOG_RECORD_TYPE_DLETED {
		return nil, KeyNotExists
	}
	return buffer.Val, nil
}
func (db *DB) Delete(key []byte) error {
	//判断key
	if len(key) == 0 {
		return KeyIsEmpty
	}
	//先查找内存中是否存在该键
	if db.indexer.Get(key) == nil {
		//若不存在该键，直接返回
		return nil
	}
	LogRecord_ := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LOG_RECORD_TYPE_DLETED,
	}
	_, err := db.AppendLogRecordWithLock(LogRecord_)
	if err != nil {
		return err
	}
	//存入内存索引
	if ok := db.indexer.Delete(key); !ok {
		return IndexUpdateFail
	}

	return nil
}
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	for _, file := range db.oldFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (db *DB) ListKeys() [][]byte {
	iterator := db.indexer.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.indexer.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.indexer.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.PosGet(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func DestroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.option.DirPath)
		if err != nil {
			panic(err)
		}
	}
}
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}
