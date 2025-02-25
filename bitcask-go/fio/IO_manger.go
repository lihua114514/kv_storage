package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIOManager(filename string) (*FileIO, error) {
	//os.MkdirAll(filename, 0744)
	fid, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
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

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
