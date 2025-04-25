package main

import (
	"errors"
	"fmt"
	bitcask_go "github.com/lihua114514/kv_storage/bitcask-go"
	"github.com/lihua114514/kv_storage/bitcask-go/redis"
	"github.com/lihua114514/kv_storage/bitcask-go/utils"
	"strings"

	"github.com/tidwall/redcon"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":     set,
	"get":     get,
	"hset":    hset,
	"sadd":    sadd,
	"lpush":   lpush,
	"zadd":    zadd,
	"exec":    exec,
	"multi":   multi,
	"discard": discard,
	"watch":   watch,
}
var supportedCommandsNoTranscation = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BitcaskClient struct {
	server      *BitcaskServer
	db          *redis.RedisDataStructure
	isTx        bool              // 是否处于事务中
	txAborted   bool              // 事务是否因错误被中止
	txCommands  []redcon.Command  // 事务命令队列
	watchedKeys map[string]uint64 // 被监视的键及其版本号
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	client, _ := conn.Context().(*BitcaskClient)

	// 处理事务中的命令
	if client.isTx {
		if client.txAborted {
			conn.WriteError("ERR Transaction contains errors")
			return
		}

		switch command {
		case "exec":
			res, err := exec(client, cmd.Args[1:])
			if err != nil {
				conn.WriteError(err.Error())
			} else {
				if res == nil {
					conn.WriteNull() // WATCH失败
				} else {
					results := res.([]interface{})
					conn.WriteArray(len(results))
					for _, item := range results {
						conn.WriteAny(item)
					}
				}
			}
			return
		case "discard":
			res, err := discard(client, cmd.Args[1:])
			if err != nil {
				conn.WriteError(err.Error())
			} else {
				conn.WriteAny(res)
			}
			return
		case "multi":
			conn.WriteError("ERR MULTI calls can not be nested")
			return
		default:
			// 检查命令参数数量
			cmdArgCount := getCommandArgCount(command)
			argCount := len(cmd.Args) - 1
			if (cmdArgCount >= 0 && argCount != cmdArgCount) || (cmdArgCount < 0 && argCount < -cmdArgCount) {
				client.txAborted = true
				conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
				return
			}

			if _, ok := supportedCommands[command]; !ok {
				client.txAborted = true
				conn.WriteError("ERR unknown command '" + command + "'")
				return
			}

			client.txCommands = append(client.txCommands, cmd)
			conn.WriteString("QUEUED")
			return
		}
	}

	// 非事务命令处理
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command: '" + command + "'")
		return
	}

	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcask_go.IndexNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

// 辅助函数：获取命令所需参数数量
func getCommandArgCount(cmd string) int {
	switch cmd {
	case "set":
		return 2
	case "get":
		return 1
	case "hset":
		return 3
	case "sadd":
		return 2
	case "lpush":
		return 2
	case "zadd":
		return 3
	case "multi":
		return 0
	case "exec":
		return 0
	case "discard":
		return 0
	case "watch":
		return -1 // 至少一个参数
	default:
		return -2 // 未知命令
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
func multi(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if cli.isTx {
		return nil, errors.New("ERR MULTI calls can not be nested")
	}
	cli.isTx = true
	cli.txAborted = false
	cli.txCommands = make([]redcon.Command, 0)
	cli.watchedKeys = make(map[string]uint64)
	return redcon.SimpleString("OK"), nil
}
func exec(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if !cli.isTx {
		return nil, errors.New("ERR EXEC without MULTI")
	}
	defer func() {
		cli.isTx = false
		cli.txCommands = nil
		cli.txAborted = false
		cli.watchedKeys = nil
	}()
	if cli.txAborted {
		return nil, errors.New("EXECABORT Transaction discarded because of previous errors.")
	}
	// 检查WATCH的键是否被修改
	if len(cli.watchedKeys) > 0 {
		for key, originalVersion := range cli.watchedKeys {
			currentVersion := cli.db.GetVersion([]byte(key))
			if currentVersion != originalVersion {
				return nil, nil // WATCH失败，返回nil
			}
		}
	}
	// 执行事务中的命令
	results := make([]interface{}, 0, len(cli.txCommands))
	for _, txCmd := range cli.txCommands {
		cmdArgs := txCmd.Args
		command := strings.ToLower(string(cmdArgs[0]))
		handler, ok := supportedCommandsNoTranscation[command]
		if !ok {
			results = append(results, errors.New("ERR unknown command '"+command+"'"))
			continue
		}
		res, err := handler(cli, cmdArgs[1:])
		if err != nil {
			results = append(results, err)
		} else {
			results = append(results, res)
		}
	}
	return results, nil
}
func discard(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if !cli.isTx {
		return nil, errors.New("ERR DISCARD without MULTI")
	}
	cli.isTx = false
	cli.txAborted = false
	cli.txCommands = nil
	cli.watchedKeys = nil
	return redcon.SimpleString("OK"), nil
}
func watch(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if cli.isTx {
		return nil, errors.New("ERR WATCH inside MULTI is not allowed")
	}
	if len(args) < 1 {
		return nil, newWrongNumberOfArgsError("watch")
	}

	for _, key := range args {
		k := string(key)
		version := cli.db.GetVersion(key)
		cli.watchedKeys[k] = version
	}
	return redcon.SimpleString("OK"), nil
}
