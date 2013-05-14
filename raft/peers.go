package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrTimeout        = errors.New("timeout")
	ErrInvalidRequest = errors.New("invalid request")
)

// Peer is anything which provides a Raft-domain interface to a Server. Peer is
// an interface to facilitate making Servers available over different transport
// mechanisms (e.g. pure local, net/rpc, Protobufs, HTTP...). All Peers should
// be 1:1 with a Server.
type Peer interface {
	Id() uint64
	AppendEntries(AppendEntries) (AppendEntriesResponse, error)
	RequestVote(RequestVote) (RequestVoteResponse, error)
}

// LocalPeer is the simplest kind of Peer, mapped to a Server in the
// same process-space. Useful for testing and demonstration; not so
// useful for networks of independent processes.
type LocalPeer struct {
	server *Server
}

func NewLocalPeer(server *Server) *LocalPeer { return &LocalPeer{server} }

func (p *LocalPeer) Id() uint64 { return p.server.Id }

func (p *LocalPeer) AppendEntries(ae AppendEntries) (AppendEntriesResponse, error) {
	b := &bytes.Buffer{}
	d := make(chan struct{})
	p.server.Incoming(RPC{
		Procedure: ae,
		Writer:    b,
		Done:      d,
	})
	<-d

	var aer AppendEntriesResponse
	err := json.NewDecoder(b).Decode(&aer)
	return aer, err
}

func (p *LocalPeer) RequestVote(rv RequestVote) (RequestVoteResponse, error) {
	b := &bytes.Buffer{}
	d := make(chan struct{})
	p.server.Incoming(RPC{
		Procedure: rv,
		Writer:    b,
		Done:      d,
	})
	<-d

	var rvr RequestVoteResponse
	err := json.NewDecoder(b).Decode(&rvr)
	return rvr, err
}

func DoRequestVote(p Peer, r RequestVote, timeout time.Duration) (RequestVoteResponse, error) {
	type tuple struct {
		Response RequestVoteResponse
		Err      error
	}
	c := make(chan tuple)
	go func() {
		rvr, err := p.RequestVote(r)
		c <- tuple{rvr, err}
	}()

	select {
	case t := <-c:
		return t.Response, t.Err
	case <-time.After(timeout):
		return RequestVoteResponse{}, ErrTimeout
	}
}

// Peers is a collection of Peer interfaces. It provides some convenience
// functions for actions that should apply to multiple Peers.
type Peers map[uint64]Peer

func (p Peers) Except(id uint64) Peers {
	except := Peers{}
	for id0, peer := range p {
		if id0 == id {
			continue
		}
		except[id0] = peer
	}
	return except
}

func (p Peers) Count() int { return len(p) }

func (p Peers) Quorum() int {
	switch n := len(p); n {
	case 0, 1:
		return 1
	default:
		return (n / 2) + 1
	}
}

// RequestVotes sends the passed RequestVote RPC to every peer in Peers. It
// forwards responses along the returned RequestVoteResponse channel. It calls
// DoRequestVote with a timeout of BroadcastInterval * 2 (chosen arbitrarily).
// Peers that don't respond within the timeout are retried forever. The retry
// loop stops only when all peers have responded, or a Cancel signal is sent via
// the returned Canceler.
func (p Peers) RequestVotes(r RequestVote) (chan RequestVoteResponse, Canceler) {
	// "[A server entering the candidate stage] issues RequestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	// construct the channels we'll return
	abortChan := make(chan struct{})
	responsesChan := make(chan RequestVoteResponse)

	// compact a peer.RequestVote response to a single struct
	type tuple struct {
		Id                  uint64
		RequestVoteResponse RequestVoteResponse
		Err                 error
	}

	go func() {
		// we loop until all Peers have given us a response
		// track which Peers have responded
		respondedAlready := Peers{} // none yet

		for {
			notYetResponded := disjoint(p, respondedAlready)
			if len(notYetResponded) <= 0 {
				return // done
			}

			// scatter
			tupleChans := map[uint64]chan tuple{}
			for id, peer := range notYetResponded {
				tupleChans[id] = make(chan tuple)
				go func(id0 uint64, peer0 Peer) {
					resp, err := DoRequestVote(peer0, r, 2*BroadcastInterval())
					tupleChans[id0] <- tuple{id0, resp, err}
				}(id, peer)
			}

			// gather
			for id, tupleChan := range tupleChans {
				select {
				case t := <-tupleChan:
					if t.Err != nil {
						continue // will need to retry
					}
					respondedAlready[id] = nil             // value irrelevant
					responsesChan <- t.RequestVoteResponse // forward the vote
				case <-abortChan:
					return
				}
			}
		}
	}()

	return responsesChan, cancel(abortChan)
}

type Canceler interface {
	Cancel()
}

type cancel chan struct{}

func (c cancel) Cancel() { close(c) }

func disjoint(all, except Peers) Peers {
	d := Peers{}
	for id, peer := range all {
		if _, ok := except[id]; ok {
			continue
		}
		d[id] = peer
	}
	return d
}
