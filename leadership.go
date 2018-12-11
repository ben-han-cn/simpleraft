package simpleraft

import (
	"github.com/hashicorp/raft"
)

type LeaderShipHandler interface {
	OnLeaderChange(leader string, members []string) error
}

type LeaderShipHandlerFn func(string, []string) error

func (f LeaderShipHandlerFn) OnLeaderChange(leader string, members []string) error {
	return f(leader, members)
}

type LeadershipObserver struct {
	eventCh <-chan raft.Observation
	leader  string
}

func (n *Node) RegisterLeadershipObserver(handler LeaderShipHandler) {
	filter := func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	}

	ch := make(chan raft.Observation)
	o := raft.NewObserver(ch, false, filter)
	n.RegisterObserver(o)
	go func() {
		defer n.DeregisterObserver(o)
		leader := ""
		for {
			observation := <-ch
			newLeader := observation.Data.(raft.LeaderObservation).Leader
			if leader != newLeader {
				leader = newLeader
				peers, err := n.peerStore.Peers()
				if err != nil {
					panic("peer store get peer failed:" + err.Error())
				}
				if err := handler.OnLeaderChange(leader, peers); err != nil {
					return
				}
			}
		}
	}()
}
