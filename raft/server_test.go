package raft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterbourgon/progol/raft"
	"log"
	"os"
	"sync/atomic"
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
	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	server := raft.NewServer(1, &bytes.Buffer{}, noop)
	server.SetPeers(raft.Peers{
		2: nonresponsivePeer(2),
		3: nonresponsivePeer(3),
	})
	server.Start()

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
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)

	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	server := raft.NewServer(1, &bytes.Buffer{}, noop)
	server.SetPeers(raft.Peers{
		2: approvingPeer(2),
		3: nonresponsivePeer(3),
	})
	server.Start()

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
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)

	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	server := raft.NewServer(1, &bytes.Buffer{}, noop)
	server.SetPeers(raft.Peers{
		2: disapprovingPeer(2),
		3: nonresponsivePeer(3),
	})
	server.Start()

	time.Sleep(2 * raft.ElectionTimeout())
	if server.State == raft.Leader {
		t.Fatalf("erroneously became Leader")
	}
}

func TestSimpleConsensus(t *testing.T) {
	type SetValue struct {
		Value int32 `json:"value"`
	}

	var i1, i2, i3 int32

	applyValue := func(i *int32) func([]byte) ([]byte, error) {
		return func(cmd []byte) ([]byte, error) {
			var sv SetValue
			if err := json.Unmarshal(cmd, &sv); err != nil {
				return []byte{}, err
			}
			atomic.StoreInt32(i, sv.Value)
			return json.Marshal(map[string]interface{}{"ok": true})
		}
	}

	s1 := raft.NewServer(1, &bytes.Buffer{}, applyValue(&i1))
	s2 := raft.NewServer(2, &bytes.Buffer{}, applyValue(&i2))
	s3 := raft.NewServer(3, &bytes.Buffer{}, applyValue(&i3))

	peers := map[uint64]raft.Peer{
		s1.Id: raft.NewLocalPeer(s1),
		s2.Id: raft.NewLocalPeer(s2),
		s3.Id: raft.NewLocalPeer(s3),
	}

	s1.SetPeers(peers)
	s2.SetPeers(peers)
	s3.SetPeers(peers)

	s1.Start()
	s2.Start()
	s3.Start()

	time.Sleep(2 * raft.ElectionTimeout())

	cmd := SetValue{42}
	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s1.Command(cmdBuf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Command response: %s", resp)

	done := make(chan struct{})
	go func() {
		d := 10 * time.Millisecond
		for {
			i1l := atomic.LoadInt32(&i1)
			i2l := atomic.LoadInt32(&i2)
			i3l := atomic.LoadInt32(&i3)
			t.Logf("i1=%02d i2=%02d i3=%02d", i1l, i2l, i3l)
			if i1l == cmd.Value && i2l == cmd.Value && i3l == cmd.Value {
				close(done)
				return
			}
			time.Sleep(d)
			d *= 2
		}
	}()

	select {
	case <-done:
		t.Logf("success")
	case <-time.After(2 * time.Second):
		t.Errorf("timeout")
	}

	t.Logf(
		"final: i1=%02d i2=%02d i3=%02d",
		atomic.LoadInt32(&i1),
		atomic.LoadInt32(&i2),
		atomic.LoadInt32(&i3),
	)
	time.Sleep(raft.ElectionTimeout())
}
