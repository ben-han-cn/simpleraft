package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/ben-han-cn/cement/serializer"
	"github.com/ben-han-cn/cement/signal"
	"github.com/ben-han-cn/simpleraft"
)

type GetCmd struct {
	Key string
}

type SetCmd struct {
	Key   string
	Value string
}

type DelCmd struct {
	Key string
}

type cmdCoder struct {
	coder *serializer.Serializer
}

func newCmdCoder() *cmdCoder {
	coder := serializer.NewSerializer()
	coder.Register(&GetCmd{})
	coder.Register(&SetCmd{})
	coder.Register(&DelCmd{})
	return &cmdCoder{
		coder: coder,
	}
}

func (c *cmdCoder) Encode(cmd simpleraft.Command) ([]byte, error) {
	return c.coder.Encode(cmd)
}

func (c *cmdCoder) Decode(b []byte) (simpleraft.Command, error) {
	return c.coder.Decode(b)
}

type DKV struct {
	values map[string]string
}

func newDKV() *DKV {
	return &DKV{
		values: make(map[string]string),
	}
}

func (store *DKV) HandleCmd(cmd simpleraft.Command) (interface{}, error) {
	switch c := cmd.(type) {
	case GetCmd:
		if value, ok := store.values[c.Key]; ok {
			return value, nil
		} else {
			return nil, errors.New("key not exists")
		}
	case SetCmd:
		store.values[c.Key] = c.Value
		return "ok", nil
	case DelCmd:
		delete(store.values, c.Key)
		return "ok", nil
	default:
		return nil, fmt.Errorf("unknown cmd %v", cmd)
	}
}

func (store *DKV) IsCmdReadOnly(cmd simpleraft.Command) bool {
	switch cmd.(type) {
	case GetCmd:
		return true
	default:
		return false
	}
}

func (store *DKV) Close() error {
	return nil
}

func (store *DKV) Restore(data []byte) error {
	values := make(map[string]string)
	err := json.Unmarshal(data, &values)
	if err == nil {
		store.values = values
	}
	return err
}

func (store *DKV) Backup() ([]byte, error) {
	return json.Marshal(store.values)
}

var (
	dir  string
	addr string
	join string
)

func init() {
	flag.StringVar(&dir, "d", ".", "directory to save snapshot")
	flag.StringVar(&addr, "i", "127.0.0.1:5553", "ip address to communicate")
	flag.StringVar(&join, "j", "", "group to join")
}

func main() {
	flag.Parse()
	transport, err := simpleraft.NewSimpleTransport(addr)
	if err != nil {
		panic(err.Error())
	}

	cfg := simpleraft.NodeConfig{
		Dir: ".",
		Tn:  transport,
	}
	node := simpleraft.New(&cfg, newDKV(), newCmdCoder())
	var peers []string
	if join != "" {
		peers = strings.Split(join, ",")
	}

	if err := node.Open(peers); err != nil {
		panic("open node failed:" + err.Error())
	}
	signal.WaitForInterrupt(nil)
}
