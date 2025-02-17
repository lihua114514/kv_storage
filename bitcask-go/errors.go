package bitcask_go

import "errors"

var (
	KeyIsEmpty           = errors.New("the key is empty")
	IndexUpdateFail      = errors.New("index update fail")
	IndexNotFound        = errors.New("index not found")
	DataFileNotExists    = errors.New("data file not exists")
	KeyNotExists         = errors.New("key not exists")
	ErrExceedMaxBatchNum = errors.New("Eceed Max BatchNum")
	DataFileERROR        = errors.New("data file error other file exists")
)
