package raft

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

var (
	ErrTermTooOld        = errors.New("term too old")
	ErrIndexTooOld       = errors.New("index too old")
	ErrCommitIndexTooOld = errors.New("commit index too old")
	ErrCommitIndexTooBig = errors.New("commit index too big")
	ErrInvalidChecksum   = errors.New("invalid checksum")
	ErrNoCommand         = errors.New("no command")
)

type Log struct {
	sync.RWMutex
	store       io.Writer
	entries     []LogEntry
	commitIndex uint64
	execute     func([]byte)
}

func NewLog(store io.Writer, execute func([]byte)) *Log {
	return &Log{
		store:       store,
		entries:     []LogEntry{},
		commitIndex: 0,
		execute:     execute,
	}
}

// LastIndex returns the index of the most recent log entry.
func (l *Log) LastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndex()
}

func (l *Log) lastIndex() uint64 {
	return uint64(len(l.entries))
}

// LastTerm returns the term of the most recent log entry.
func (l *Log) LastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTerm()
}

func (l *Log) lastTerm() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// AppendEntry appends the passed log entry to the log.
// It will return an error if any condition is violated.
func (l *Log) AppendEntry(entry LogEntry) error {
	l.Lock()
	defer l.Unlock()
	return l.appendEntry(entry)
}

func (l *Log) appendEntry(entry LogEntry) error {
	if len(l.entries) > 0 {
		lastTerm := l.lastTerm()
		if entry.Term < lastTerm {
			return ErrTermTooOld
		}
		if entry.Term == lastTerm && entry.Index <= l.lastIndex() {
			return ErrIndexTooOld
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

// CommitTo commits all log entries up to the passed commitIndex. Commit means:
// synchronize the log entry to persistent storage, and call the state machine
// execute function for the log entry's command.
func (l *Log) CommitTo(commitIndex uint64) error {
	l.Lock()
	defer l.Unlock()
	return l.commitTo(commitIndex)
}

func (l *Log) commitTo(commitIndex uint64) error {
	// Reject old commit indexes
	if commitIndex <= l.commitIndex {
		return ErrCommitIndexTooOld
	}

	// Reject new commit indexes
	if commitIndex > uint64(len(l.entries)) {
		return ErrCommitIndexTooBig
	}

	// Sync entries between our commit index and the passed commit index
	for i := l.commitIndex; i < commitIndex; i++ {
		entry := l.entries[i]
		if err := entry.Encode(l.store); err != nil {
			return err
		}
		l.execute(entry.Command)
		l.commitIndex = entry.Index
	}

	return nil
}

// LogEntry is the atomic unit being managed by the distributed log.
// A LogEntry always has an index (monotonically increasing), a Term in which
// the Raft network leader first sees the entry, and a Command. The Command is
// what gets executed against the Raft node's state machine when the LogEntry
// is replicated throughout the Raft network.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"` // when received by leader
	Command []byte `json:"command,omitempty"`
}

// Encode serializes the log entry to the passed io.Writer.
func (e *LogEntry) Encode(w io.Writer) error {
	if len(e.Command) <= 0 {
		return ErrNoCommand
	}
	buf := &bytes.Buffer{}
	if _, err := fmt.Fprintf(buf, "%016x %016x %s\n", e.Index, e.Term, e.Command); err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	_, err := fmt.Fprintf(w, "%08x %s", checksum, buf.String())
	return err
}

// Decode deserializes the log entry from the passed io.Reader.
// Decode returns the number of bytes read.
func (e *LogEntry) Decode(r io.Reader) (int, error) {
	pos := 0

	// Read the expected checksum first.
	var readChecksum uint32
	if _, err := fmt.Fscanf(r, "%08x", &readChecksum); err != nil {
		return pos, err
	}
	pos += 8

	// Read the rest of the line.
	rd := bufio.NewReader(r)
	if c, _ := rd.ReadByte(); c != ' ' {
		return pos, fmt.Errorf("LogEntry: Decode: expected space, got %02x", c)
	}
	pos += 1

	line, err := rd.ReadString('\n')
	pos += len(line)
	if err == io.EOF {
		return pos, err
	} else if err != nil {
		return pos, err
	}
	b := bytes.NewBufferString(line)

	computedChecksum := crc32.ChecksumIEEE(b.Bytes())
	if readChecksum != computedChecksum {
		return pos, ErrInvalidChecksum
	}

	if _, err = fmt.Fscanf(b, "%016x %016x ", &e.Index, &e.Term); err != nil {
		return pos, fmt.Errorf("LogEntry: Decode: scan: %s", err)
	}
	e.Command, err = b.ReadBytes('\n')
	if err != nil {
		return pos, err
	}
	bytes.TrimSpace(e.Command)

	// Make sure there's only an EOF remaining.
	c, err := b.ReadByte()
	if err != io.EOF {
		return pos, fmt.Errorf("LogEntry: Decode: expected EOF, got %02x", c)
	}

	return pos, nil
}
