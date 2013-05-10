package raft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterbourgon/progol/raft"
	"net/http"
	"testing"
	"time"
)

type nonresponsivePeer uint64

func (p nonresponsivePeer) Id() uint64                  { return uint64(p) }
func (p nonresponsivePeer) Call([]byte) ([]byte, error) { return []byte{}, nil }

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

func ExampleServerOverHTTP() {
	server := raft.NewServer(123, &bytes.Buffer{}, func([]byte) {})
	// server.SetPeers(...)

	decodeAndForward := func(p interface{}) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(p); err != nil {
				http.Error(
					w,
					fmt.Sprintf(`{"error": "%s"}`, err),
					http.StatusBadRequest,
				)
				return
			}

			d := make(chan struct{})
			server.Incoming(raft.RPC{
				Procedure: p,
				Writer:    w,
				Done:      d,
			})
			<-d
		}
	}

	http.HandleFunc("/append-entries", decodeAndForward(&raft.AppendEntries{}))
	http.HandleFunc("/request-vote", decodeAndForward(&raft.RequestVote{}))
	// http.ListenAndServe(addr, nil)
}
