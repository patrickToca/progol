package raft_test

import (
	"bytes"
	"github.com/peterbourgon/progol/raft"
)

func ExampleLocalCluster() {
	noop := func([]byte) ([]byte, error) { return []byte{}, nil }

	s1 := raft.NewServer(1, &bytes.Buffer{}, noop)
	s2 := raft.NewServer(2, &bytes.Buffer{}, noop)
	s3 := raft.NewServer(3, &bytes.Buffer{}, noop)

	peers := raft.Peers{
		s1.Id: raft.NewLocalPeer(s1),
		s2.Id: raft.NewLocalPeer(s2),
		s3.Id: raft.NewLocalPeer(s3),
	}

	s1.SetPeers(peers)
	s2.SetPeers(peers)
	s3.SetPeers(peers)
}
