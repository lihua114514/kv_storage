package main

import (
	bitcask_go "github.com/lihua114514/kv_storage/bitcask-go"
	"github.com/lihua114514/kv_storage/bitcask-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	redisDataStructure, err := redis.NewRedisDataStructur(bitcask_go.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务端
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}
