package redis

import (
	"encoding/binary"
	"errors"
	bitcask_go "kv_storage/bitcask-go"
	"time"
)

type redisDataType byte

var (
	ErrWrongTypeOperation = errors.New("wrong type Operation")
)

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	db *bitcask_go.DB
}

func NewRedisDataStructur(options bitcask_go.Options) (*RedisDataStructure, error) {
	db, err := bitcask_go.Open(options)
	if err != nil {
		return nil, err

	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// redis数据结构
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if key == nil {
		return nil
	}
	// 编码 value : type(类型) + expire（过期时间） + payload（value内容）
	buffer := make([]byte, binary.MaxVarintLen64+1)

	buffer[0] = byte(String)
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buffer[index:], expire)

	encVal := make([]byte, index+len(value))
	copy(encVal[:index], buffer[:index])
	copy(encVal[index:], value)
	return rds.db.Put(key, encVal)
}
func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, bitcask_go.KeyIsEmpty
	}
	val, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	type_get := val[0]
	if type_get != byte(String) {

		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(val[index:])
	index += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return val[index:], nil

}
