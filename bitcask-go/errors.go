package bitcask_go

import "errors"

var (
	KeyIsEmpty               = errors.New("the key is empty")
	IndexUpdateFail          = errors.New("index update fail")
	IndexNotFound            = errors.New("index not found")
	DataFileNotExists        = errors.New("data file not exists")
	KeyNotExists             = errors.New("key not exists")
	ErrExceedMaxBatchNum     = errors.New("Eceed Max BatchNum")
	DataFileERROR            = errors.New("data file error other file exists")
	ErrMergeIsProgress       = errors.New("Merge is progressing")
	ErrMergeRatioUnreached   = errors.New("MergeRatio is unreached")
	ErrNoEnoughSpaceForMerge = errors.New("No enough space")
	ErrDataBaseIsUsing       = errors.New("DataBase is using ")
)
