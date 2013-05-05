package progol

import (
	"errors"
	"log"
	"sync"
	"time"
)

var (
	ErrInvalidStateMachine = errors.New("invalid state machine in application")
)

// Every application built on progol is necessarily a state machine. Progol apps
// must deal with at least two types of events: changes to the network from a
// Validator, and proposals.
//
// State represents a discrete operating state of an application. Each State is
// defined as a label, plus a set of transition functions for incoming events.
type State struct {
	Label       string
	OnEndpoints func([]Endpoint) (string, error) // next state
	OnProposal  func([]byte) (string, error)     // next state
}

// The Registry defines the complete set of states for an application.
type Registry map[string]State

// Add adds a new State to a Registry. It performs no duplication checking.
func (r Registry) Add(s State) {
	(map[string]State)(r)[s.Label] = s
}

// A Machine is an agent that uses a Registry to map incoming events to
// application States.
type Machine struct {
	sync.Mutex
	registry Registry
	current  string
	changes  chan StateChange
}

// A StateChange is a signal to the application that the State Machine requires
// a transition from 'from' to 'to'. The State Machine assumes every StateChange
// signal will be respected.
type StateChange struct {
	From string
	To   string
}

// NewMachine uses a Registry to map incoming events to application states.
// It will listen to network change events from the passed Validator.
// You should install the machine's AcceptProposal handler at ProposalHandler.
// The machine will signal state changes on the passed channel.
func NewMachine(r Registry, startingState string, v *Validator, c chan StateChange) *Machine {
	m := &Machine{
		registry: r,
		current:  startingState,
		changes:  c,
	}

	e := make(chan []Endpoint)
	go translate(e, m)
	v.Subscribe(e)

	return m
}

// AcceptProposal links a State Machine to incoming Proposals.
// It returns a function that should be passed to ProposalHandler.
func (m *Machine) AcceptProposal() func([]byte) error {
	return func(proposal []byte) error {
		m.Lock()
		defer m.Unlock()

		s, ok := m.registry[m.current]
		if !ok {
			return ErrInvalidStateMachine
		}
		next, err := s.OnProposal(proposal)
		if err != nil {
			return err
		}
		if _, ok := m.registry[next]; !ok {
			return ErrInvalidStateMachine
		}

		select {
		case m.changes <- StateChange{m.current, next}:
			break
		case <-time.After(10 * time.Millisecond):
			panic("Machine: AcceptProposal: uncoöperative receiver")
		}

		m.current = next
		return nil
	}
}

// handleEndpoints processes the passed endpoints (presumably a state change
// from the associated Validator) and potentially effects a state change in the
// Machine.
func (m *Machine) handleEndpoints(endpoints []Endpoint) error {
	m.Lock()
	defer m.Unlock()

	s, ok := m.registry[m.current]
	if !ok {
		return ErrInvalidStateMachine
	}
	next, err := s.OnEndpoints(endpoints)
	if err != nil {
		return err
	}
	if _, ok := m.registry[next]; !ok {
		return ErrInvalidStateMachine
	}

	select {
	case m.changes <- StateChange{m.current, next}:
		break
	case <-time.After(10 * time.Millisecond):
		panic("Machine: handleEndpoints: uncoöperative receiver")
	}

	m.current = next
	return nil
}

// translate calls the Machine's handleEndpoints for every incoming set of
// Endpoints.
func translate(c chan []Endpoint, m *Machine) {
	for endpoints := range c {
		if err := m.handleEndpoints(endpoints); err != nil {
			log.Printf("Machine: new endpoints (%v): %s", endpoints, err)
		}
	}
}
