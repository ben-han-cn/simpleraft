package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/ben-han-cn/simpleraft"
	"github.com/ben-han-cn/simpleraft/webservice"
)

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

	cfg := simpleraft.NodeConfig{
		Addr:   addr,
		Dir:    ".",
		Logger: log.New(ioutil.Discard, "", log.LstdFlags),
	}

	node := simpleraft.New(&cfg, nil, nil)
	var peers []string
	if join != "" {
		peers = strings.Split(join, ",")
	}

	if err := node.Open(peers); err != nil {
		panic("open node failed:" + err.Error())
	}

	var handler simpleraft.LeaderShipHandlerFn
	handler = func(leader string, members []string) error {
		fmt.Printf("!!!!! leader: %s, members: %v\n", leader, members)
		return nil
	}
	node.RegisterLeadershipObserver(handler)

	server := webservice.New(node, httpaddr)
	server.Run()
}
