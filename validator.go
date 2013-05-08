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

// Endpoint represents an ideal peer, and whether or not it was verified.
type Endpoint struct {
	Peer url.URL
	OK   bool
}

// Validator continuously validates a set of ideal peers from a Discovery,
// and continuously publishes the results of those validations to subscribers.
// Subscribers must take care to maintain their subscription channel.
type Validator struct {
	in          chan []url.URL
	interval    time.Duration
	timeout     time.Duration
	broadcasts  chan []Endpoint
	subscribe   chan chan []Endpoint
	unsubscribe chan chan []Endpoint
	subscribers map[chan []Endpoint]struct{}
}

func NewValidator(d Discovery, interval time.Duration) *Validator {
	v := &Validator{
		in:          make(chan []url.URL),
		interval:    interval,
		broadcasts:  make(chan []Endpoint),
		subscribe:   make(chan chan []Endpoint),
		unsubscribe: make(chan chan []Endpoint),
		subscribers: map[chan []Endpoint]struct{}{},
	}
	go v.loop()
	d.Subscribe(v.in)
	return v
}

// Subscribe registers the passed channel to receive updates when the set of
// valid Endpoints changes.
func (v *Validator) Subscribe(c chan []Endpoint) {
	v.subscribe <- c
}

// Unsubscribe unregisters the passed channel so that it will no longer receive
// updates when the set of valid Endpoints changes.
func (v *Validator) Unsubscribe(c chan []Endpoint) {
	v.unsubscribe <- c
}

func (v *Validator) loop() {
	quit := make(chan struct{}) // signal to our validation cycle
	last := []url.URL{}         // most recent set of ideal peers

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

func copySubscribers(m map[chan []Endpoint]struct{}) []chan []Endpoint {
	a := []chan []Endpoint{}
	for c := range m {
		a = append(a, c)
	}
	return a
}

func broadcastEndpoints(subscribers []chan []Endpoint, endpoints []Endpoint) {
	for _, subscriber := range subscribers {
		select {
		case subscriber <- endpoints:
			break
		case <-time.After(10 * time.Millisecond):
			panic("uncoöperative Validator client")
		}
	}
}

// CmpPeers returns true if the passed URL slices are the same (contain the same
// URLs, independent of order) and false otherwise.
func CmpPeers(a, b []url.URL) bool {
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

func cycle(quit chan struct{}, peers []url.URL, interval time.Duration, out chan []Endpoint) {
	timeout := interval / 2
	last := pingAll(peers, timeout)
	go func() { out <- last }() // initial signal
	t := time.Tick(interval)
	for {
		select {
		case <-quit:
			return

		case <-t:
			endpoints := pingAll(peers, timeout)
			if CmpEndpoints(endpoints, last) {
				continue
			}
			go func() { out <- endpoints }()
			last = endpoints
		}
	}
}

// CmpEndpoints returns true if the passed Endpoints slices are identical
// (contain the same Endpoints at the same state, independent of order) and
// false otherwise.
func CmpEndpoints(a, b []Endpoint) bool {
	if len(a) != len(b) {
		return false
	}

	s := func(e Endpoint) string { return e.Peer.String() + fmt.Sprint(e.OK) }

	aStr := sort.StringSlice{}
	for _, e := range a {
		aStr = append(aStr, s(e))
	}
	sort.Sort(aStr)

	bStr := sort.StringSlice{}
	for _, e := range b {
		bStr = append(bStr, s(e))
	}
	sort.Sort(bStr)

	for i, _ := range aStr {
		if aStr[i] != bStr[i] {
			return false
		}
	}
	return true
}

func pingAll(peers []url.URL, timeout time.Duration) []Endpoint {
	// scatter
	chans := make([]chan Endpoint, len(peers))
	for i, peer := range peers {
		chans[i] = make(chan Endpoint)
		go func(i0 int, peer0 url.URL) {
			chans[i0] <- pingOne(peer0, timeout)
		}(i, peer)
	}

	// gather
	endpoints := make([]Endpoint, len(peers))
	for i, _ := range peers {
		endpoints[i] = <-chans[i]
	}

	return endpoints
}

func pingOne(peer url.URL, timeout time.Duration) Endpoint {
	e := make(chan error)
	go func() { e <- ping(peer) }()

	select {
	case <-time.After(timeout):
		return Endpoint{peer, false}
	case err := <-e:
		return Endpoint{peer, err == nil}
	}
	panic("unreachable")
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
