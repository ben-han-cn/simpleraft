package simpleraft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount     = 2
	defalutHeartbeetTimeout = 1 * time.Second
	defaultApplyTimeout     = 10 * time.Second
	defaultOpenTimeout      = 120 * time.Second
	leaderWaitDelay         = 100 * time.Millisecond
	appliedWaitDelay        = 100 * time.Millisecond
)

type Transport interface {
	net.Listener
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

type Node struct {
	raftDir       string
	raft          *raft.Raft // The consensus mechanism.
	raftTransport Transport
	peerStore     raft.PeerStore

	logger *log.Logger

	SnapshotThreshold uint64
	HeartbeatTimeout  time.Duration
	ApplyTimeout      time.Duration
	OpenTimeout       time.Duration

	service Service
	coder   CommandCoder
}

type NodeConfig struct {
	Dir    string
	Tn     Transport
	Logger *log.Logger
}

func New(c *NodeConfig, service Service, coder CommandCoder) *Node {
	logger := c.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[store] ", log.LstdFlags)
	}

	return &Node{
		raftDir:          c.Dir,
		raftTransport:    c.Tn,
		logger:           logger,
		HeartbeatTimeout: defalutHeartbeetTimeout,
		ApplyTimeout:     defaultApplyTimeout,
		OpenTimeout:      defaultOpenTimeout,
		service:          service,
		coder:            coder,
	}
}

func (n *Node) Open(peers []string) error {
	if err := os.MkdirAll(n.raftDir, 0755); err != nil {
		return err
	}

	transport := raft.NewNetworkTransport(n.raftTransport, 3, 10*time.Second, os.Stderr)

	n.peerStore = raft.NewJSONPeers(n.raftDir, transport)
	if len(peers) != 0 {
		n.peerStore.SetPeers(peers)
	}

	config := n.raftConfig()

	peers, err := n.peerStore.Peers()
	if err != nil {
		return err
	}

	if len(peers) <= 1 {
		n.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	snapshots, err := raft.NewFileSnapshotStore(n.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %n", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(n.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %n", err)
	}

	ra, err := raft.NewRaft(config, n, logStore, logStore, snapshots, n.peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %n", err)
	}
	n.raft = ra

	return n.WaitForAppliedIndex(n.raft.LastIndex(), n.OpenTimeout)
}

func (n *Node) Close(wait bool) error {
	if err := n.service.Close(); err != nil {
		return err
	}
	f := n.raft.Shutdown()
	if wait {
		if e := f.(raft.Future); e.Error() != nil {
			return e.Error()
		}
	}
	return nil
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) State() raft.RaftState {
	return n.raft.State()
}

func (n *Node) Path() string {
	return n.raftDir
}

func (n *Node) Addr() net.Addr {
	return n.raftTransport.Addr()
}

func (n *Node) Leader() string {
	return n.raft.Leader()
}

func (n *Node) Peers() ([]string, error) {
	return n.peerStore.Peers()
}

func (n *Node) WaitForLeader(timeout time.Duration) (string, error) {
	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			l := n.Leader()
			if l != "" {
				return l, nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

func (n *Node) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if n.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

func (n *Node) Execute(c Command) *FSMGenericResponse {
	if n.service.IsCmdReadOnly(c) {
		result, err := n.service.HandleCmd(c)
		return &FSMGenericResponse{
			Err:    err,
			Result: result,
		}
	}

	if n.raft.State() != raft.Leader {
		return &FSMGenericResponse{
			Err: raft.ErrNotLeader,
		}
	}

	b, err := n.coder.Encode(c)
	if err != nil {
		return &FSMGenericResponse{
			Err: err,
		}
	}

	f := n.raft.Apply(b, n.ApplyTimeout)
	if err := f.(raft.Future).Error(); err != nil {
		return &FSMGenericResponse{
			Err: err,
		}
	}

	return f.Response().(*FSMGenericResponse)
}

func (n *Node) Join(addr string) error {
	n.logger.Printf("received request to join node at %s", addr)
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	f := n.raft.AddPeer(addr)
	if e := f.(raft.Future); e.Error() != nil {
		return e.Error()
	}
	n.logger.Printf("node at %s joined successfully", addr)
	return nil
}

func (n *Node) Remove(addr string) error {
	n.logger.Printf("received request to remove node %n", addr)

	f := n.raft.RemovePeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	n.logger.Printf("node %n removed successfully", addr)
	return nil
}

func (n *Node) raftConfig() *raft.Config {
	config := raft.DefaultConfig()
	if n.SnapshotThreshold != 0 {
		config.SnapshotThreshold = n.SnapshotThreshold
	}
	if n.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = n.HeartbeatTimeout
		config.ElectionTimeout = 10 * n.HeartbeatTimeout
	}
	return config
}

type FSMGenericResponse struct {
	Err    error
	Result interface{}
}

func (n *Node) Apply(l *raft.Log) interface{} {
	if l.Data == nil {
		return &FSMGenericResponse{Err: fmt.Errorf("empty log")}
	}

	c, err := n.coder.Decode(l.Data)
	if err != nil {
		return &FSMGenericResponse{Err: fmt.Errorf("decode command failed:%s", err.Error())}
	}

	result, err := n.service.HandleCmd(c)
	return &FSMGenericResponse{
		Err:    err,
		Result: result,
	}
}

func (n *Node) Snapshot() (raft.FSMSnapshot, error) {
	fsmState, err := n.service.Backup()
	if err != nil {
		n.logger.Printf("failed to read fsmState for snapshot: %n", err.Error())
		return nil, err
	} else {
		return &fsmSnapshot{
			fsmState: fsmState,
		}, nil
	}
}

func (n *Node) Restore(rc io.ReadCloser) error {
	if err := n.service.Close(); err != nil {
		return err
	}

	var sz uint64
	if err := binary.Read(rc, binary.LittleEndian, &sz); err != nil {
		return err
	}

	fsmState := make([]byte, sz)
	if _, err := io.ReadFull(rc, fsmState); err != nil {
		return err
	}

	return n.service.Restore(fsmState)
}

func (n *Node) RegisterObserver(o *raft.Observer) {
	n.raft.RegisterObserver(o)
}

func (n *Node) DeregisterObserver(o *raft.Observer) {
	n.raft.DeregisterObserver(o)
}

type fsmSnapshot struct {
	fsmState []byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b := new(bytes.Buffer)
		sz := uint64(len(f.fsmState))
		err := binary.Write(b, binary.LittleEndian, sz)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b.Bytes()); err != nil {
			return err
		}

		if _, err := sink.Write(f.fsmState); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
