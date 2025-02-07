package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIOManager(filename string) (*FileIO, error) {
	fid, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	} else {
		return &FileIO{fd: fid}, nil
	}
}
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}
func (fio *FileIO) Read(p []byte, offset int64) (n int, err error) {
	return fio.fd.ReadAt(p, offset)
}
func (fio *FileIO) Write(p []byte) (n int, err error) {
	return fio.fd.Write(p)
}

//初始化I/O方法
