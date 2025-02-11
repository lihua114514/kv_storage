package bitcask_go

import (
	"os"
	"testing"
)

func destroyDB(db *DB) {
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
func TestOpen(t *testing.T) {

}
