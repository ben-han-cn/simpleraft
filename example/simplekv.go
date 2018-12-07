package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/ben-han-cn/cement/serializer"
	"github.com/ben-han-cn/simpleraft"
	"github.com/ben-han-cn/simpleraft/webservice"
	"github.com/julienschmidt/httprouter"
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

type SimpleKV struct {
	values map[string]string
}

func newSimpleKV() *SimpleKV {
	return &SimpleKV{
		values: make(map[string]string),
	}
}

func (store *SimpleKV) HandleCmd(cmd simpleraft.Command) (interface{}, error) {
	switch c := cmd.(type) {
	case *GetCmd:
		if value, ok := store.values[c.Key]; ok {
			return value, nil
		} else {
			return nil, errors.New("key not exists")
		}
	case *SetCmd:
		store.values[c.Key] = c.Value
		return nil, nil
	case *DelCmd:
		delete(store.values, c.Key)
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown cmd %v", cmd)
	}
}

func (store *SimpleKV) IsCmdReadOnly(cmd simpleraft.Command) bool {
	switch cmd.(type) {
	case GetCmd:
		return true
	default:
		return false
	}
}

func (store *SimpleKV) Close() error {
	return nil
}

func (store *SimpleKV) Restore(data []byte) error {
	values := make(map[string]string)
	err := json.Unmarshal(data, &values)
	if err == nil {
		store.values = values
	}
	return err
}

func (store *SimpleKV) Backup() ([]byte, error) {
	return json.Marshal(store.values)
}

var (
	dir      string
	addr     string
	join     string
	httpaddr string
)

func init() {
	flag.StringVar(&dir, "d", ".", "directory to save snapshot")
	flag.StringVar(&httpaddr, "s", "127.0.0.1:6553", "http server address")
	flag.StringVar(&addr, "i", "127.0.0.1:5553", "ip address to communicate raft")
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

	kv := newSimpleKV()
	node := simpleraft.New(&cfg, kv, newCmdCoder())
	var peers []string
	if join != "" {
		peers = strings.Split(join, ",")
	}

	if err := node.Open(peers); err != nil {
		panic("open node failed:" + err.Error())
	}

	server := webservice.New(node, httpaddr)
	runCmd := func(cmd simpleraft.Command, w http.ResponseWriter) {
		result := node.Execute(cmd)
		if result.Err != nil {
			webservice.EncodeResult(w, webservice.Failed(result.Err.Error()))
		} else if result.Result == nil {
			webservice.EncodeResult(w, webservice.Succeed())
		} else {
			webservice.EncodeResult(w, webservice.SucceedWithResult(&struct {
				Value string `json:"value"`
			}{result.Result.(string)}))
		}
	}

	server.RegisterHandler("GET", "/db/:key", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		key := ps.ByName("key")
		runCmd(&GetCmd{Key: key}, w)
	})

	server.RegisterHandler("DELETE", "/db/:key", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		key := ps.ByName("key")
		runCmd(&DelCmd{Key: key}, w)
	})

	server.RegisterHandler("POST", "/db", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		param := struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{}
		err := webservice.DecodeRequestBody(r, &param)
		if err != nil {
			webservice.EncodeResult(w, webservice.Failed("request body fmt isn't valid"))
			return
		}
		runCmd(&SetCmd{Key: param.Key, Value: param.Value}, w)
	})
	server.Run()
}
