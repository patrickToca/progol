package raft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterbourgon/progol/raft"
	"net/http"
)

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
