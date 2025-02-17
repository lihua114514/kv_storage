package main

import (
	"fmt"
	bitcask_go "kv_storage/bitcask-go"
)

func main() {
	opts := bitcask_go.DefaultOptions
	opts.DirPath = "/home/lihua/kv_storage/bitcask-go/test_data"
	db, err := bitcask_go.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
