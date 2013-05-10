// +build slow

package raft_test

import (
	"bytes"
	"fmt"
	"github.com/peterbourgon/progol/raft"
	"log"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lmicroseconds)
}

type nonresponsivePeer uint64

func (p nonresponsivePeer) Id() uint64 { return uint64(p) }
func (p nonresponsivePeer) AppendEntries(raft.AppendEntries) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, fmt.Errorf("not responding")
}
func (p nonresponsivePeer) RequestVote(raft.RequestVote) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, fmt.Errorf("not responding")
}

func TestFollowerToCandidate(t *testing.T) {
	server := raft.NewServer(1, &bytes.Buffer{}, func([]byte) {})
	server.SetPeers(raft.Peers{
		2: nonresponsivePeer(2),
		3: nonresponsivePeer(3),
	})

	if server.State != raft.Follower {
		t.Fatalf("didn't start as Follower")
	}

	d := 2 * raft.ElectionTimeout()
	time.Sleep(d)

	if server.State != raft.Candidate {
		t.Fatalf("after %s, not Candidate", d.String())
	}
}

type approvingPeer uint64

func (p approvingPeer) Id() uint64 { return uint64(p) }
func (p approvingPeer) AppendEntries(raft.AppendEntries) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, fmt.Errorf("not implemented")
}
func (p approvingPeer) RequestVote(rv raft.RequestVote) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{
		Term:        rv.Term,
		VoteGranted: true,
	}, nil
}

func TestCandidateToLeader(t *testing.T) {
	server := raft.NewServer(1, &bytes.Buffer{}, func([]byte) {})
	server.SetPeers(raft.Peers{
		2: approvingPeer(2),
		3: nonresponsivePeer(3),
	})
	t.Logf("BroadcastInterval = %s", raft.BroadcastInterval())
	time.Sleep(2 * raft.ElectionTimeout())
	if server.State != raft.Leader {
		t.Fatalf("didn't become Leader")
	}
}

type disapprovingPeer uint64

func (p disapprovingPeer) Id() uint64 { return uint64(p) }
func (p disapprovingPeer) AppendEntries(raft.AppendEntries) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, fmt.Errorf("not implemented")
}
func (p disapprovingPeer) RequestVote(rv raft.RequestVote) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{
		Term:        rv.Term,
		VoteGranted: false,
	}, nil
}

func TestFailedElection(t *testing.T) {
	server := raft.NewServer(1, &bytes.Buffer{}, func([]byte) {})
	server.SetPeers(raft.Peers{
		2: disapprovingPeer(2),
		3: nonresponsivePeer(3),
	})
	time.Sleep(2 * raft.ElectionTimeout())
	if server.State == raft.Leader {
		t.Fatalf("erroneously became Leader")
	}
}
