package raft

import (
	"encoding/json"
	"io"
)

type RPC struct {
	Procedure interface{}   // AppendEntries or RequestVote
	Writer    io.Writer     // where the Server will write the Response
	Done      chan struct{} // signal the Server is finished
}

func (rpc *RPC) Request() interface{} {
	return rpc.Procedure
}

func (rpc *RPC) Respond(resp interface{}) error {
	err := json.NewEncoder(rpc.Writer).Encode(resp)
	close(rpc.Done)
	return err
}

type AppendEntries struct {
	Term         uint64     `json:"term"`
	LeaderId     uint64     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	CommitIndex  uint64     `json:"commit_index"`
}

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	reason  string
}

type RequestVote struct {
	Term         uint64 `json:"term"`
	CandidateId  uint64 `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	reason      string
}
