package raft_test

import (
	"bytes"
	"github.com/peterbourgon/progol/raft"
	"math"
	"strings"
	"testing"
)

func TestLogEntryEncodeDecode(t *testing.T) {
	for _, logEntry := range []raft.LogEntry{
		raft.LogEntry{0, 0, []byte(`{}`)},
		raft.LogEntry{0, 1, []byte(`{}`)},
		raft.LogEntry{1, 0, []byte(`{}`)},
		raft.LogEntry{1, 1, []byte(`{}`)},
		raft.LogEntry{255, 3, []byte(`{"cmd": 123}`)},
		raft.LogEntry{math.MaxUint64 - 1, math.MaxUint64, []byte(`{}`)},
	} {
		b := &bytes.Buffer{}
		if err := logEntry.Encode(b); err != nil {
			t.Errorf("%v: Encode: %s", logEntry, err)
			continue
		}
		t.Logf("%v: Encode: %s", logEntry, strings.TrimSpace(b.String()))
		sz := b.Len()

		var e raft.LogEntry
		if n, err := e.Decode(b); err != nil {
			t.Errorf("%v: Decode: %s", logEntry, err)
		} else if n != sz {
			t.Errorf("%v: Decode: expected %d, decoded %d", logEntry, sz, n)
		}

	}
}

func TestLogAppend(t *testing.T) {
	c := []byte(`{}`)
	buf := &bytes.Buffer{}
	noop := func([]byte) {}
	log := raft.NewLog(buf, noop)

	// Append 3 valid LogEntries
	if err := log.AppendEntry(raft.LogEntry{1, 1, c}); err != nil {
		t.Errorf("Append: %s", err)
	}
	if err := log.AppendEntry(raft.LogEntry{2, 1, c}); err != nil {
		t.Errorf("Append: %s", err)
	}
	if err := log.AppendEntry(raft.LogEntry{3, 2, c}); err != nil {
		t.Errorf("Append: %s", err)
	}

	// Append some invalid LogEntries
	if err := log.AppendEntry(raft.LogEntry{4, 1, c}); err != raft.ErrTermTooOld {
		t.Errorf("Append: expected ErrTermTooOld, got %v", err)
	}
	if err := log.AppendEntry(raft.LogEntry{2, 2, c}); err != raft.ErrIndexTooOld {
		t.Errorf("Append: expected ErrIndexTooOld, got %v", nil)
	}

	// Check our flush buffer, before doing any commits
	precommit := ``
	if expected, got := precommit, buf.String(); expected != got {
		t.Errorf("before commit, expected:\n%s\ngot:\n%s\n", expected, got)
	}

	// Commit the first two, only
	if err := log.CommitTo(2); err != nil {
		t.Errorf("CommitTo: %s", err)
	}

	// Check our flush buffer
	lines := []string{
		`02869b0c 0000000000000001 0000000000000001 {}`,
		`3bfe364c 0000000000000002 0000000000000001 {}`,
	}
	if expected, got := strings.Join(lines, "\n")+"\n", buf.String(); expected != got {
		t.Errorf("after commit, expected:\n%s\ngot:\n%s\n", expected, got)
	}

	// Make some invalid commits
	if err := log.CommitTo(1); err != raft.ErrCommitIndexTooOld {
		t.Errorf("Commit: expected ErrCommitIndexTooOld, got %v", err)
	}
	if err := log.CommitTo(2); err != raft.ErrCommitIndexTooOld {
		t.Errorf("Commit: expected ErrCommitIndexTooOld, got %v", err)
	}
	if err := log.CommitTo(4); err != raft.ErrCommitIndexTooBig {
		t.Errorf("Commit: expected ErrCommitIndexTooBig, got %v", err)
	}

	// Commit every LogEntry
	if err := log.CommitTo(3); err != nil {
		t.Errorf("CommitTo: %s", err)
	}

	// Check our flush buffer again
	lines = append(
		lines,
		`6b76285c 0000000000000003 0000000000000002 {}`,
	)
	if expected, got := strings.Join(lines, "\n")+"\n", buf.String(); expected != got {
		t.Errorf("after commit, expected:\n%s\ngot:\n%s\n", expected, got)
	}
}
