package progol

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"
)

const (
	ValidationPath = "/progol/ping"
)

// ValidationHandler certifies a peer valid, when installed at ValidationPath.
func ValidationHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Peer represents a node on the network.
type Peer struct {
	URL url.URL // HTTP
	OK  bool    // validated
}

// Validator continuously validates a set of ideal peers from a Discovery,
// and continuously publishes the results of those validations to subscribers.
// Subscribers must take care to maintain their subscription channel.
type Validator struct {
	in          chan []Peer
	interval    time.Duration
	timeout     time.Duration
	broadcasts  chan []Peer
	subscribe   chan chan []Peer
	unsubscribe chan chan []Peer
	subscribers map[chan []Peer]struct{}
}

func NewValidator(d Discovery, interval time.Duration) *Validator {
	v := &Validator{
		in:          make(chan []Peer), // un-validated Peers
		interval:    interval,
		broadcasts:  make(chan []Peer),
		subscribe:   make(chan chan []Peer),
		unsubscribe: make(chan chan []Peer),
		subscribers: map[chan []Peer]struct{}{},
	}
	go v.loop()
	d.Subscribe(v.in)
	return v
}

// Subscribe registers the passed channel to receive updates when the set of
// valid Endpoints changes.
func (v *Validator) Subscribe(c chan []Peer) {
	v.subscribe <- c
}

// Unsubscribe unregisters the passed channel so that it will no longer receive
// updates when the set of valid Endpoints changes.
func (v *Validator) Unsubscribe(c chan []Peer) {
	v.unsubscribe <- c
}

func (v *Validator) loop() {
	quit := make(chan struct{}) // signal to our validation cycle
	last := []Peer{}            // most recent set of ideal (unverified) peers

	for {
		select {
		case peers := <-v.in:
			if CmpPeers(peers, last) {
				continue
			}

			close(quit)
			quit = make(chan struct{})
			go cycle(quit, peers, v.interval, v.broadcasts)
			last = peers

		case endpoints := <-v.broadcasts:
			go broadcastEndpoints(copySubscribers(v.subscribers), endpoints)

		case c := <-v.subscribe:
			if _, ok := v.subscribers[c]; !ok {
				v.subscribers[c] = struct{}{}
			}

		case c := <-v.unsubscribe:
			if _, ok := v.subscribers[c]; ok {
				delete(v.subscribers, c)
			}
		}
	}
}

func copySubscribers(m map[chan []Peer]struct{}) []chan []Peer {
	a := []chan []Peer{}
	for c := range m {
		a = append(a, c)
	}
	return a
}

func broadcastEndpoints(subscribers []chan []Peer, peers []Peer) {
	for _, subscriber := range subscribers {
		select {
		case subscriber <- peers:
			break
		case <-time.After(10 * time.Millisecond):
			panic("uncoöperative Validator client")
		}
	}
}

// CmpUrls returns true if the passed URL slices are the same (contain the same
// URLs, independent of order) and false otherwise.
func CmpUrls(a, b []url.URL) bool {
	if len(a) != len(b) {
		return false
	}

	aStr := sort.StringSlice{}
	for _, u := range a {
		aStr = append(aStr, u.String())
	}
	sort.Sort(aStr)

	bStr := sort.StringSlice{}
	for _, u := range b {
		bStr = append(bStr, u.String())
	}
	sort.Sort(bStr)

	for i, _ := range aStr {
		if aStr[i] != bStr[i] {
			return false
		}
	}
	return true
}

func cycle(quit chan struct{}, peers []Peer, interval time.Duration, out chan []Peer) {
	timeout := interval / 2
	last := pingAll(peers, timeout)
	go func() { out <- last }() // initial signal
	t := time.Tick(interval)
	for {
		select {
		case <-quit:
			return

		case <-t:
			next := pingAll(peers, timeout)
			if CmpPeers(next, last) {
				continue
			}
			go func() { out <- next }()
			last = next
		}
	}
}

// CmpPeers returns true if the passed Peer slices are identical
// (contain the same Peers at the same state, independent of order) and
// false otherwise.
func CmpPeers(a, b []Peer) bool {
	if len(a) != len(b) {
		return false
	}

	s := func(p Peer) string { return p.URL.String() + fmt.Sprint(p.OK) }

	aStr := sort.StringSlice{}
	for _, p := range a {
		aStr = append(aStr, s(p))
	}
	sort.Sort(aStr)

	bStr := sort.StringSlice{}
	for _, p := range b {
		bStr = append(bStr, s(p))
	}
	sort.Sort(bStr)

	for i, _ := range aStr {
		if aStr[i] != bStr[i] {
			return false
		}
	}
	return true
}

func pingAll(peers []Peer, timeout time.Duration) []Peer {
	// scatter
	chans := make([]chan Peer, len(peers))
	for i, p := range peers {
		chans[i] = make(chan Peer)
		go func(i0 int, url0 url.URL) {
			chans[i0] <- pingOne(url0, timeout)
		}(i, p.URL)
	}

	// gather
	newPeers := make([]Peer, len(peers))
	for i, _ := range peers {
		newPeers[i] = <-chans[i]
	}

	return newPeers
}

func pingOne(url url.URL, timeout time.Duration) Peer {
	e := make(chan error)
	go func() { e <- ping(url) }()

	select {
	case <-time.After(timeout):
		return Peer{url, false}
	case err := <-e:
		return Peer{url, err == nil}
	}
}

func ping(peer url.URL) error {
	peer.Path = ValidationPath
	resp, err := http.Get(peer.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
