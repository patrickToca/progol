package raft

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

const (
	MinimumElectionTimeoutMs = 250
)

func ElectionTimeout() time.Duration {
	n := rand.Intn(MinimumElectionTimeoutMs)
	d := MinimumElectionTimeoutMs + n
	return time.Duration(d) * time.Millisecond
}

func BroadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMs / 10
	return time.Duration(d) * time.Millisecond
}

type Server struct {
	Id           uint64 // of this server, for elections and redirects
	State        string
	Term         uint64 // "current term number, which increases monotonically"
	vote         uint64 // who we voted for this term, if applicable
	Log          *Log
	peers        Peers
	peersChan    chan Peers
	rpcChan      chan RPC
	commandChan  chan []byte
	electionTick <-chan time.Time
}

func NewServer(id uint64, store io.Writer, apply func([]byte) ([]byte, error)) *Server {
	if id <= 0 {
		panic("server id must be > 0")
	}

	s := &Server{
		Id:           id,
		State:        Follower, // "when servers start up they begin as followers"
		Term:         1,        // TODO is this correct?
		Log:          NewLog(store, apply),
		peers:        nil,
		peersChan:    make(chan Peers),
		rpcChan:      make(chan RPC),
		commandChan:  make(chan []byte),
		electionTick: time.NewTimer(ElectionTimeout()).C, // one-shot
	}
	go s.loop()
	return s
}

// TODO Command accepts client commands, which will (hopefully) get replicated
// across all state machines. Note that Command is completely out-of-band of
// Raft-domain RPC.

func (s *Server) Incoming(rpc RPC) {
	s.rpcChan <- rpc
}

func (s *Server) SetPeers(p Peers) {
	s.peersChan <- p
}

//                                  times out,
//                                 new election
//     |                             .-----.
//     |                             |     |
//     v         times out,          |     v     receives votes from
// +----------+  starts election  +-----------+  majority of servers  +--------+
// | Follower |------------------>| Candidate |---------------------->| Leader |
// +----------+                   +-----------+                       +--------+
//     ^ ^                              |                                 |
//     | |    discovers current leader  |                                 |
//     | |                 or new term  |                                 |
//     | '------------------------------'                                 |
//     |                                                                  |
//     |                               discovers server with higher term  |
//     '------------------------------------------------------------------'
//
//

func (s *Server) loop() {
	for {
		switch s.State {
		case Follower:
			s.followerSelect()
		case Candidate:
			s.candidateSelect()
		case Leader:
			s.leaderSelect()
		default:
			panic(fmt.Sprintf("unknown Server State in loop: %s", s.State))
		}
	}
}

func (s *Server) resetElectionTimeout() {
	s.electionTick = time.NewTimer(ElectionTimeout()).C
}

func (s *Server) logGeneric(format string, args ...interface{}) {
	prefix := fmt.Sprintf("id=%d term=%d state=%s: ", s.Id, s.Term, s.State)
	log.Printf(prefix+format, args...)
}

func (s *Server) logAppendEntriesResponse(r AppendEntriesResponse, stepDown bool) {
	s.logGeneric(
		"got AppendEntries: success=%v (%s) stepDown=%v",
		r.Success,
		r.reason,
		stepDown,
	)
}
func (s *Server) logRequestVoteResponse(r RequestVoteResponse, stepDown bool) {
	s.logGeneric(
		"got RequestVote: granted=%v (%s) stepDown=%v",
		r.VoteGranted,
		r.reason,
		stepDown,
	)
}

func (s *Server) followerSelect() {
	select {
	case p := <-s.peersChan:
		s.peers = p
		return

	case <-s.commandChan:
		// TODO fwd caller to leader somehow
		return

	case <-s.electionTick:
		// 5.2 Leader election: "A follower increments its current term and
		// transitions to candidate state."
		s.logGeneric("election timeout, becoming candidate")
		s.Term++
		s.State = Candidate
		s.resetElectionTimeout()
		return

	case rpc := <-s.rpcChan:
		switch r := rpc.Request().(type) {
		case AppendEntries:
			resp, stepDown := s.handleAppendEntries(r)
			s.logAppendEntriesResponse(resp, stepDown)
			rpc.Respond(resp)
		case RequestVote:
			resp, stepDown := s.handleRequestVote(r)
			s.logRequestVoteResponse(resp, stepDown)
			rpc.Respond(resp)
		}
	}
}

func (s *Server) candidateSelect() {
	// "[A server entering the candidate stage] issues RequestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	responses, canceler := s.peers.RequestVotes(RequestVote{
		Term:         s.Term,
		CandidateId:  s.Id,
		LastLogIndex: s.Log.LastIndex(),
		LastLogTerm:  s.Log.LastTerm(),
	})
	defer canceler.Cancel()
	votesReceived := 1 // already have a vote from myself
	votesRequired := s.peers.Quorum()
	s.logGeneric("election started, %d vote(s) required", votesRequired)

	// catch a bad state
	if votesReceived >= votesRequired {
		s.logGeneric("%d-node cluster; I win", s.peers.Count())
		s.State = Leader
		return
	}

	// "A candidate continues in this state until one of three things happens:
	// (a) it wins the election, (b) another server establishes itself as
	// leader, or (c) a period of time goes by with no winner."
	for {
		select {
		case p := <-s.peersChan:
			// just bookkeeping, but note it's only applicable for the next
			// round, as we've already locked in our votesRequired
			s.peers = p
			continue

		case <-s.commandChan:
			// TODO fwd caller to leader somehow
			continue

		case r := <-responses:
			s.logGeneric("got vote: term=%d granted=%v", r.Term, r.VoteGranted)
			// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if r.Term != s.Term {
				// TODO what if r.Term > s.Term? do we lose the election?
				continue
			}
			if r.VoteGranted {
				votesReceived++
			}
			// "Once a candidate wins an election, it becomes leader."
			if votesReceived >= votesRequired {
				s.logGeneric("%d >= %d: win", votesReceived, votesRequired)
				s.State = Leader
				return // win
			}

		case rpc := <-s.rpcChan:
			switch r := rpc.Request().(type) {
			case AppendEntries:
				// "While waiting for votes, a candidate may receive an
				// AppendEntries RPC from another server claiming to be leader.
				// If the leader's term (included in its RPC) is at least as
				// large as the candidate's current term, then the candidate
				// recognizes the leader as legitimate and steps down, meaning
				// that it returns to follower state."
				resp, stepDown := s.handleAppendEntries(r)
				s.logAppendEntriesResponse(resp, stepDown)
				rpc.Respond(resp)
				if stepDown {
					s.logGeneric("stepping down to Follower")
					s.State = Follower
					return // lose
				}
			case RequestVote:
				// We can also be defeated by a more recent candidate
				resp, stepDown := s.handleRequestVote(r)
				s.logRequestVoteResponse(resp, stepDown)
				rpc.Respond(resp)
				if stepDown {
					s.logGeneric("stepping down to Follower")
					s.State = Follower
					return // lose
				}
			}

		case <-s.electionTick: //  "a period of time goes by with no winner"
			s.logGeneric("election ended with no winner")
			s.resetElectionTimeout()
			return // draw
		}
	}
}

func resetNextIndex(nextIndex map[uint64]uint64, peers Peers, idx uint64) {
	for _, peer := range peers {
		if _, ok := nextIndex[peer.Id()]; !ok {
			nextIndex[peer.Id()] = idx
		}
	}
}

func (s *Server) leaderSelect() {
	// 5.3 Log replication
	// "Once a leader has been elected, it begins servicing client requests.
	// Each client request contains a command that must eventually be executed
	// by the replicated state machines. The leader appends the command to its
	// log as a new entry, then issues AppendEntries RPCs in parallel to each
	// of the other servers to replicate the entry. When the leader decides that
	// a log entry is committed, it applies the entry to its state machine and
	// returns the result of that execution to the client. If followers crash or
	// run slowly, or if network packets are lost, the leader retries
	// AppendEntries RPCs indefinitely (even after it has responsed to the
	// client) until all followers eventually store all log entries.

	// 5.3 Log replication: "The leader maintains a nextIndex for each follower,
	// which is the index of the next log entry the leader will send to that
	// follower. When a leader first comes to power it initializes all nextIndex
	// values to the index just after the last one in its log."
	nextIndex := map[uint64]uint64{} // followerId: nextIndex
	resetNextIndex(nextIndex, s.peers, s.Log.LastIndex()+1)

	heartbeatTick := time.Tick(BroadcastInterval())
	for {
		select {
		case p := <-s.peersChan:
			s.peers = p
			resetNextIndex(nextIndex, s.peers, s.Log.LastIndex()+1)
			continue

		case cmd := <-s.commandChan:
			_, err := s.replicateCommand(cmd, nextIndex)
			if err != nil {
				// TODO
			}
			// TODO fwd resp somehow

		case <-heartbeatTick:
			s.logGeneric("sending heartbeat to %d", s.peers.Count())
			s.peers.BroadcastHeartbeat(s.Term, s.Id) // TODO manage responses?
			continue

		case rpc := <-s.rpcChan:
			switch r := rpc.Request().(type) {
			case AppendEntries:
				resp, stepDown := s.handleAppendEntries(r)
				s.logAppendEntriesResponse(resp, stepDown)
				rpc.Respond(resp)
				if stepDown {
					s.State = Follower
					return // ousted
				}
			case RequestVote:
				resp, stepDown := s.handleRequestVote(r)
				s.logRequestVoteResponse(resp, stepDown)
				rpc.Respond(resp)
				if stepDown {
					s.State = Follower
					return // ousted
				}
			}
		}
	}
}

func disjoint2(all Peers, except map[uint64]AppendEntriesResponse) Peers {
	d := Peers{}
	for id, peer := range all {
		if _, ok := except[id]; ok {
			continue
		}
		d[id] = peer
	}
	return d
}

func (s *Server) replicateCommand(cmd []byte, nextIndex map[uint64]uint64) ([]byte, error) {
	// 5.3 Log replication: "Each client request contains a command that must
	// eventually be executed by the replicated state machines. The leader
	// appends the command to its log as a new entry,"
	if err := s.Log.AppendEntry(LogEntry{
		Index:   s.Log.LastIndex() + 1,
		Term:    s.Term,
		Command: cmd,
	}); err != nil {
		return []byte{}, err
	}

	// "then issues AppendEntries RPCs in parallel to each of the other servers
	// to replicate the entry."
	results := map[uint64]AppendEntriesResponse{}
	replicationTimeoutOne := BroadcastInterval() * 2 // TODO arbitrary

	// TODO this work should somehow continue indefinitely, rather than being
	// aborted after a "hard limit" timeout. But, that means it should be
	// modeled as some "replication" actor, which can absorb new units of
	// replication work as the situation changes.
	hardLimit := time.After(BroadcastInterval() * 10)

	for {
		remainingPeers := disjoint2(s.peers, results)

		// scatter
		type tuple struct {
			FollowerId uint64
			Response   AppendEntriesResponse
			Err        error
		}
		tupleChans := []chan tuple{}
		for _, peer := range remainingPeers {
			c := make(chan tuple)
			tupleChans = append(tupleChans, c)
			go func(peer0 Peer) {
				prevLogIndex := nextIndex[peer0.Id()]
				entries, prevLogTerm := s.Log.EntriesAfter(prevLogIndex)

				c0 := make(chan tuple)
				go func() {
					resp, err := peer0.AppendEntries(AppendEntries{
						Term:         s.Term,
						LeaderId:     s.Id, // always me
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						CommitIndex:  s.Log.CommitIndex(),
					})
					c0 <- tuple{peer0.Id(), resp, err}
				}()

				select {
				case c <- <-c0:
					break
				case <-time.After(replicationTimeoutOne):
					c <- tuple{peer0.Id(), AppendEntriesResponse{}, ErrTimeout}
				}
			}(peer)
		}

		// gather
		for _, tupleChan := range tupleChans {
			tuple := <-tupleChan

			// Unknown failure
			if tuple.Err != nil {
				s.logGeneric(
					"replicate command to %d: %s (will retry, same nextIndex)",
					tuple.FollowerId,
					tuple.Err,
				)
				continue
			}

			// Rejection
			if !tuple.Response.Success {
				// "After a rejection, the leader decrements nextIndex and
				// retries the AppendEntries RPC."
				if nextIndex[tuple.FollowerId] == 0 {
					panic("an AppendEntries with nextIndex == 0 failed")
				}
				nextIndex[tuple.FollowerId]--
				s.logGeneric(
					"replicate command to %d: failed (will retry w/ nextIndex=%d)",
					tuple.FollowerId,
					tuple.Err,
					nextIndex[tuple.FollowerId],
				)
				continue
			}

			// Weird condition we should nevertheless check for: Success, and:
			if tuple.Response.Term != s.Term {
				panic(fmt.Sprintf(
					"presumably impossible: success=%v term=%d myTerm=%d",
					tuple.Response.Success,
					tuple.Response.Term,
					s.Term,
				))
			}

			// Success
			results[tuple.FollowerId] = tuple.Response // Success = true
		}

		// "When the leader decides that a log entry is committed, it applies
		// the entry to its state machine and returns the result of that
		// execution to the client."
		canBeCommitted := len(results) >= s.peers.Quorum()
		if canBeCommitted {
			if err := s.Log.CommitTo(s.Log.LastIndex()); err != nil {
				return []byte{}, err
			}
			return s.Log.apply(cmd)
		}

		// TODO this is not a valid mechanism of giving up
		select {
		case <-hardLimit:
			s.logGeneric("replication attempts timed out; aborting")
			return []byte{}, ErrTimeout
		default:
			break // try again...
		}
	}
	panic("unreachable")
}

func (s *Server) handleRequestVote(r RequestVote) (RequestVoteResponse, bool) {
	// Spec is ambiguous here; basing this (loosely!) on benbjohnson's impl

	// If the request is from an old term, reject
	if r.Term < s.Term {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", r.Term, s.Term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.Term > s.Term {
		s.Term = r.Term
		s.vote = 0
		stepDown = true
	}

	// If we've already voted for someone else this term, reject
	if s.vote != 0 && s.vote != r.CandidateId {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if s.Log.LastIndex() > r.LastLogIndex || s.Log.LastTerm() > r.LastLogTerm {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			reason: fmt.Sprintf(
				"our index/term %d/%d > %d/%d",
				s.Log.LastIndex(),
				s.Log.LastTerm(),
				r.LastLogIndex,
				r.LastLogTerm,
			),
		}, stepDown
	}

	// We passed all the tests: cast vote in favor
	s.vote = r.CandidateId
	s.resetElectionTimeout() // TODO why?
	return RequestVoteResponse{
		Term:        s.Term,
		VoteGranted: true,
	}, stepDown
}

func (s *Server) handleAppendEntries(r AppendEntries) (AppendEntriesResponse, bool) {
	// Spec is ambiguous here; basing this on benbjohnson's impl

	// Maybe a nicer way to handle this is to define explicit handler functions
	// for each Server state. Then, we won't try to hide too much logic (i.e.
	// too many protocol rules) in one code path.

	// If the request is from an old term, reject
	if r.Term < s.Term {
		return AppendEntriesResponse{
			Term:    s.Term,
			Success: false,
			reason:  fmt.Sprintf("Term %d < %d", r.Term, s.Term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.Term > s.Term {
		s.Term = r.Term
		s.vote = 0
		stepDown = true
	}

	// In any case, reset our election timeout
	s.resetElectionTimeout()

	// Reject if log doesn't contain a matching previous entry
	if err := s.Log.EnsureLastIs(r.PrevLogIndex, r.PrevLogTerm); err != nil {
		return AppendEntriesResponse{
			Term:    s.Term,
			Success: false,
			reason: fmt.Sprintf(
				"while ensuring last log entry had index=%d term=%d: error: %s",
				r.PrevLogIndex,
				r.PrevLogTerm,
				err,
			),
		}, stepDown
	}

	// Append entries to the log
	for i, entry := range r.Entries {
		if err := s.Log.AppendEntry(entry); err != nil {
			return AppendEntriesResponse{
				Term:    s.Term,
				Success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(r.Entries),
					err,
				),
			}, stepDown
		}
	}

	// Commit up to the commit index
	if err := s.Log.CommitTo(r.CommitIndex); err != nil {
		return AppendEntriesResponse{
			Term:    s.Term,
			Success: false,
			reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
		}, stepDown
	}

	// all good
	return AppendEntriesResponse{
		Term:    s.Term,
		Success: true,
	}, stepDown
}
