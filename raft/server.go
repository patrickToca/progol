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
	return time.Duration(MinimumElectionTimeoutMs/20) * time.Millisecond
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
	electionTick <-chan time.Time
}

func NewServer(id uint64, store io.Writer, execute func([]byte)) *Server {
	if id <= 0 {
		panic("server id must be > 0")
	}

	s := &Server{
		Id:           id,
		State:        Follower, // "when servers start up they begin as followers"
		Term:         1,        // TODO is this correct?
		Log:          NewLog(store, execute),
		peers:        nil,
		peersChan:    make(chan Peers),
		rpcChan:      make(chan RPC),
		electionTick: time.NewTimer(ElectionTimeout()).C, // one-shot
	}
	go s.loop()
	return s
}

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
		r.Reason,
		stepDown,
	)
}
func (s *Server) logRequestVoteResponse(r RequestVoteResponse, stepDown bool) {
	s.logGeneric(
		"got RequestVote: granted=%v (%s) stepDown=%v",
		r.VoteGranted,
		r.Reason,
		stepDown,
	)
}

func (s *Server) followerSelect() {
	select {
	case p := <-s.peersChan:
		s.peers = p
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
	votesRequired := (s.peers.Count() / 2) + 1
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

		case r := <-responses:
			s.logGeneric("got vote: term=%d granted=%v", r.Term, r.VoteGranted)
			// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if r.Term != s.Term {
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
					s.State = Follower
					return // lose
				}
			case RequestVote:
				// We can also be defeated by a more recent candidate
				resp, stepDown := s.handleRequestVote(r)
				s.logRequestVoteResponse(resp, stepDown)
				rpc.Respond(resp)
				if stepDown {
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

	heartbeatTick := time.NewTimer(BroadcastInterval()).C
	select {
	case p := <-s.peersChan:
		s.peers = p
		return // TODO manage heartbeatTick

	case <-heartbeatTick:
		s.logGeneric("heartbeat to %d", s.peers.Count())
		s.peers.BroadcastHeartbeat(s.Term, s.Id) // TODO manage responses?
		return

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

func (s *Server) handleRequestVote(r RequestVote) (RequestVoteResponse, bool) {
	// Spec is ambiguous here; basing this (loosely!) on benbjohnson's impl

	// If the request is from an old term, reject
	if r.Term < s.Term {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			Reason:      fmt.Sprintf("Term %d < %d", r.Term, s.Term),
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
			Reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if s.Log.LastIndex() > r.LastLogIndex || s.Log.LastTerm() > r.LastLogTerm {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			Reason: fmt.Sprintf(
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

	// If the request is from an old term, reject
	if r.Term < s.Term {
		return AppendEntriesResponse{
			Term:    s.Term,
			Success: false,
			Reason:  fmt.Sprintf("Term %d < %d", r.Term, s.Term),
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
	if r.PrevLogIndex != s.Log.LastIndex() || r.PrevLogTerm != s.Log.LastTerm() {
		return AppendEntriesResponse{
			Term:    s.Term,
			Success: false,
			Reason: fmt.Sprintf(
				"prev index/term %d/%d != %d/%d",
				r.PrevLogIndex,
				r.PrevLogTerm,
				s.Log.LastIndex(),
				s.Log.LastTerm(),
			),
		}, stepDown
	}

	// Append entries to the log
	for i, entry := range r.Entries {
		if err := s.Log.AppendEntry(entry); err != nil {
			return AppendEntriesResponse{
				Term:    s.Term,
				Success: false,
				Reason: fmt.Sprintf(
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
			Reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
		}, stepDown
	}

	// all good
	return AppendEntriesResponse{
		Term:    s.Term,
		Success: true,
	}, stepDown
}
