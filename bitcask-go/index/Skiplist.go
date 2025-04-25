package index

import (
	"bytes"
	//"fmt"
	"math/rand"
	"time"

	"github.com/lihua114514/kv_storage/bitcask-go/data"
)

const MAXLEVEL int = 10

type Skiplist struct {
	head *Node
	// 用于随机生成level的种子，控制跳表的高度
	rand *rand.Rand
}

type Node struct {
	key  []byte
	pos  *data.LogRecordPos
	next []*Node
}

type SkiplistIterator struct {
	skiplist *Skiplist
	current  *Node
	level    int
}

func (sl *Skiplist) Iterator(reverse bool) Iterator {
	if sl == nil {
		return sl.NewIterator(false)
	} else {
		return sl.NewIterator(false)
	}
}

// 查找一个键在文件中的位置
func (sl *Skiplist) Get(key []byte) *data.LogRecordPos {
	if node := sl.Search(key); node != nil {
		return node.pos
	}
	return nil
}
func (sl *Skiplist) Search(key []byte) *Node {
	curr := sl.head
	for i := len(curr.next) - 1; i >= 0; i-- {
		if curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) == -1 {
			curr = curr.next[i]
		}
		// 如果 key 值相等，则找到了目标直接返回
		if curr.next[i] != nil && bytes.Equal(curr.next[i].key, key) {
			return curr.next[i]
		}
	}
	return nil
}

// 插入一个新的元素
func (sl *Skiplist) Put(key []byte, pos *data.LogRecordPos) bool {
	if node := sl.Search(key); node != nil {
		node.pos = pos
		return true
	}
	level := sl.randomLevel()

	//fmt.Printf("key:%s,level is %d\n", key, level)

	for len(sl.head.next)-1 < level {
		sl.head.next = append(sl.head.next, nil)
	}
	// 创建出新的节点
	newNode := Node{
		key:  key,
		pos:  pos,
		next: make([]*Node, level+1),
	}

	// 从头节点的最高层出发
	move := sl.head
	for level := level; level >= 0; level-- {
		// 向右遍历，直到右侧节点不存在或者 key 值大于 key
		for move.next[level] != nil && bytes.Compare(move.next[level].key, key) == -1 {
			move = move.next[level]
		}
		// 调整指针关系，完成新节点的插入
		newNode.next[level] = move.next[level]
		move.next[level] = &newNode
	}

	//fmt.Printf("%s\n", newNode.key)

	return true
}

// 删除一个键
func (sl *Skiplist) Delete(key []byte) bool {
	node := sl.Search(key)
	if node == nil {
		return true
	}
	// 从头节点的最高层出发
	move := sl.head
	for level := len(sl.head.next) - 1; level >= 0; level-- {
		// 向右遍历，直到右侧节点不存在或者 key 值大于等于 key
		for move.next[level] != nil && bytes.Compare(move.next[level].key, key) == -1 {
			move = move.next[level]
		}

		// 右侧节点不存在或者 key 值大于 target，则直接跳过
		if move.next[level] == nil || bytes.Compare(move.next[level].key, key) == 1 {
			continue
		}

		// 走到此处意味着右侧节点的 key 值必然等于 key，则调整指针引用
		move.next[level] = move.next[level].next[level]
	}
	// 对跳表的最大高度进行更新
	var dif int
	// 倘若某一层已经不存在数据节点，高度需要递减
	for level := len(sl.head.next) - 1; level > 0 && sl.head.next[level] == nil; level-- {
		dif++
	}
	sl.head.next = sl.head.next[:len(sl.head.next)-dif]

	return true
}

// 生成随机的level
func (sl *Skiplist) randomLevel() int {
	level := 1
	for sl.rand.Intn(2) == 0 {
		level++
	}
	return level

}
func NewSkipList() *Skiplist {
	// 初始化跳表
	skipList := &Skiplist{
		head: &Node{
			key:  nil,
			pos:  nil,
			next: make([]*Node, 1),
		},
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return skipList

}

func (sl *Skiplist) NewIterator(reverse bool) *SkiplistIterator {
	return &SkiplistIterator{
		skiplist: sl,
		current:  sl.head.next[0],
		level:    len(sl.head.next),
	}
}
func (sl *Skiplist) Size() int {
	var num int = 0
	p := sl.head.next[0]
	for p != nil {
		num++
	}
	return num
}

// 找到key的位置
func (it *SkiplistIterator) Seek(key []byte) {
	it.current = it.skiplist.head
	it.current = it.skiplist.Search(key)
}

// 检查是否有效
func (it *SkiplistIterator) Valid() bool {
	return it.current != nil
}

// 重定位
func (it *SkiplistIterator) Rewind() {
	it.current = it.skiplist.head.next[0]
}

// 返回键
func (it *SkiplistIterator) Key() []byte {
	if it.Valid() {
		return it.current.key
	}
	return nil
}

// 给出key所对应的文件中的位置
func (it *SkiplistIterator) Value() *data.LogRecordPos {
	if it.Valid() {
		return it.current.pos
	}
	return nil
}

// 跳转到下一个
func (it *SkiplistIterator) Next() {
	if it.Valid() && it.current.next[0] != nil {
		it.current = it.current.next[0]
	} else {
		it.current = nil
	}
}

func (it *SkiplistIterator) Close() {

}
