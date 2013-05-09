package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"time"
)

var (
	ErrTimeout        = errors.New("timeout")
	ErrInvalidRequest = errors.New("invalid request")
)

// Peer provides an RPC interface to a Server.
type Peer interface {
	Id() uint64
	Call([]byte) ([]byte, error) // TODO maybe not the best interface
}

// LocalPeer is the simplest kind of Peer.
// It should be 1:1 with a Server.
type LocalPeer struct {
	server *Server
}

func NewLocalPeer(server *Server) *LocalPeer {
	return &LocalPeer{
		server: server,
	}
}

func (p *LocalPeer) Id() uint64 { return p.server.Id }

func (p *LocalPeer) Call(req []byte) ([]byte, error) {
	var ae AppendEntries
	if err := json.Unmarshal(req, &ae); err != nil && ae.Term > 0 {
		b := &bytes.Buffer{}
		d := make(chan struct{})
		p.server.Incoming(RPC{
			Procedure: ae,
			Writer:    b,
			Done:      d,
		})
		<-d
		return b.Bytes(), nil
	}

	var rv RequestVote
	if err := json.Unmarshal(req, &rv); err != nil && rv.Term > 0 {
		b := &bytes.Buffer{}
		d := make(chan struct{})
		p.server.Incoming(RPC{
			Procedure: rv,
			Writer:    b,
			Done:      d,
		})
		<-d
		return b.Bytes(), nil
	}

	return []byte{}, ErrInvalidRequest
}

func DoRequestVote(p Peer, r RequestVote) (RequestVoteResponse, error) {
	timeout := 25 * time.Millisecond

	type tuple struct {
		Response []byte
		Err      error
	}
	reqBuf, _ := json.Marshal(r)
	c := make(chan tuple)
	go func() {
		resp, err := p.Call(reqBuf)
		c <- tuple{resp, err}
	}()

	select {
	case t := <-c:
		if t.Err != nil {
			return RequestVoteResponse{}, t.Err
		}
		var resp RequestVoteResponse
		if err := json.Unmarshal(t.Response, &resp); err != nil {
			return RequestVoteResponse{}, err
		}
		return resp, nil

	case <-time.After(timeout):
		return RequestVoteResponse{}, ErrTimeout
	}
	panic("unreachable")
}

// Peers is a collection of Peer interfaces. It provides some convenience
// functions for actions that should apply to multiple Peers.
type Peers map[uint64]Peer

func (p Peers) Count() int { return len(p) }

func (p Peers) BroadcastHeartbeat(term, leaderId uint64) {
	command, _ := json.Marshal(AppendEntries{
		Term:     term,
		LeaderId: leaderId,
	})

	for id, peer := range p {
		// TODO is it OK this is synchronous and serial?
		// TODO is it OK to ignore responses?
		if _, err := peer.Call(command); err != nil {
			log.Printf(
				"BroadcastHeartbeat: term=%d leader=%d: to=%d: %s",
				term,
				leaderId,
				id,
				err,
			)
		}
	}
}

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
					resp, err := DoRequestVote(peer0, r)
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
