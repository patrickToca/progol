package main

import (
	"encoding/json"
	"fmt"
	"sync"
)

// The state machine goes from an empty state, to one where this process
// has claimed a unique segment of the underlying data file.

type stateMachine struct {
	sync.Mutex

	id       uint64
	filesz   int64
	checksum string
	maxmem   int64

	seq              uint64
	checksumOK       bool
	alreadyClaimedTo int64 // index

	pending  chan struct{}
	myOffset int64
	myLength int64
}

func newStateMachine(id uint64, filesz int64, checksum string, maxmem int64) (*stateMachine, error) {
	return &stateMachine{
		id:       id,
		filesz:   filesz,
		checksum: checksum,
		maxmem:   maxmem,
		pending:  make(chan struct{}),
	}, nil
}

type applyResponse struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

func makeResponse(ok bool, reasonFmt string, args ...interface{}) []byte {
	buf, _ := json.Marshal(applyResponse{
		OK:     ok,
		Reason: fmt.Sprintf(reasonFmt, args...),
	})
	return buf
}

func (sm *stateMachine) Apply(seq uint64, cmd []byte) []byte {
	sm.Lock()
	defer sm.Unlock()

	if seq < sm.seq {
		return makeResponse(false, "too old")
	}
	if seq == sm.seq {
		return makeResponse(true, "duplicate, ignored")
	}
	if seq > sm.seq+1 {
		return makeResponse(false, "missed some commands")
	}
	if seq != sm.seq+1 {
		panic("impossible")
	}
	sm.seq = seq
	return sm.handleClaim(cmd)
}

type claim struct {
	Owner    uint64 `json:"owner"`
	Seq      uint64 `json:"seq"`
	Checksum string `json:"checksum"`
	Offset   int64  `json:"offset"`
	Length   int64  `json:"length"`
}

func (sm *stateMachine) handleClaim(cmd []byte) []byte {
	var c claim
	if err := json.Unmarshal(cmd, &c); err != nil {
		return makeResponse(false, err.Error())
	}

	if c.Seq != sm.seq {
		return makeResponse(false, "claim not matching sequence")
	}
	if c.Checksum != sm.checksum {
		return makeResponse(false, "bad checksum: local '%s', remote '%s'", sm.checksum, c.Checksum)
	}
	if c.Offset != sm.alreadyClaimedTo {
		return makeResponse(false, "claim offset=%d, but already claimed to=%d", c.Offset, sm.alreadyClaimedTo)
	}

	sz := c.Offset + c.Length
	if sz > sm.filesz {
		return makeResponse(false, "claim offset+length=%d > file size=%d", sz, sm.filesz)
	}

	if c.Owner == sm.id {
		fmt.Printf("--> I own offset=%d length=%d\n", c.Offset, c.Length)
		sm.myOffset = c.Offset
		sm.myLength = c.Length
		close(sm.pending)
	}

	sm.alreadyClaimedTo = sz
	return makeResponse(true, "")
}

func (sm *stateMachine) MakeClaim() []byte {
	sm.Lock()
	defer sm.Unlock()

	offset, length := sm.alreadyClaimedTo, sm.maxmem
	if offset+length > sm.filesz {
		length = sm.filesz - offset
	}
	buf, _ := json.Marshal(claim{
		Owner:    sm.id,
		Seq:      sm.seq + 1, // what it should arrive as
		Checksum: sm.checksum,
		Offset:   offset,
		Length:   length,
	})

	return buf
}

func (sm *stateMachine) Segment() (int64, int64) {
	<-sm.pending
	return sm.myOffset, sm.myLength
}
