package progol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	ProposalPath = "/progol/propose"
)

var (
	ErrPeerNotValidated = errors.New("peer not validated")
)

// Proposal describes a state change that a peer wants to make.
type Proposal struct {
	Peers       []Endpoint    // 100% approval is required
	Description interface{}   // JSON-marshaled and sent to peers for approval
	Timeout     time.Duration // time after which Proposal automatically fails
}

// Propose submits a Proposal to all peers. The Proposal passes if all peers
// accept it, and fails if any peer rejects it.
func (p *Proposal) Propose() error {
	responses := make([]chan error, len(p.Peers))
	for i, peer := range p.Peers {
		responses[i] = make(chan error)
		go func(i0 int, peer0 Endpoint) {
			responses[i0] <- propose(p.Description, peer0, p.Timeout)
		}(i, peer)
	}

	errors := []string{}
	for _, responseChan := range responses {
		if err := <-responseChan; err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf(strings.Join(errors, "; "))
	}
	return nil
}

func propose(description interface{}, peer Endpoint, timeout time.Duration) error {
	if !peer.OK {
		return ErrPeerNotValidated
	}

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(description); err != nil {
		return err
	}

	peer.Peer.Path = ProposalPath
	req, err := http.NewRequest("POST", peer.Peer.String(), buf)
	if err != nil {
		return fmt.Errorf("rejected by %s (%s)", peer.Peer.Host, err)
	}

	c := &http.Client{
		Transport: &http.Transport{
			Dial: timeoutDialer(timeout),
		},
	}
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("rejected by %s (%s)", peer.Peer.Host, err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"rejected by %s (HTTP %d) (%s)",
			peer.Peer.Host,
			resp.StatusCode,
			extractReason(resp),
		)
	}

	return nil
}

// ProposalHandler returns a http.HandlerFunc that uses the passed acceptance
// function to validate incoming Proposals. ProposalHandler should be installed
// at ProposalPath.
func ProposalHandler(accept func([]byte) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buf, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Printf("ProposalHandler: %s", buf)
		defer r.Body.Close()
		if err := accept(buf); err != nil {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func timeoutDialer(d time.Duration) func(netw, addr string) (net.Conn, error) {
	return func(netw, addr string) (net.Conn, error) {
		c, err := net.Dial(netw, addr)
		if err != nil {
			return nil, err
		}
		c.SetDeadline(time.Now().Add(d))
		return c, nil
	}
}

func extractReason(resp *http.Response) string {
	var e struct {
		reason string `json:"error"`
	}
	defer resp.Body.Close()
	if json.NewDecoder(resp.Body).Decode(&e) == nil {
		return e.reason
	}
	return "unspecified reason"
}
