package progol

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// Endpoint represents an ideal peer, and whether or not it was verified.
type Endpoint struct {
	Peer *url.URL
	OK   bool
}

// Validator continuously validates a set of ideal peers from a Discovery,
// and continuously publishes the results of those validations to subscribers.
// Subscribers must take care to maintain their subscription channel.
type Validator struct {
	in            chan []*url.URL
	interval      time.Duration
	timeout       time.Duration
	broadcasts    chan []Endpoint
	subscriptions chan chan []Endpoint
	subscribers   []chan []Endpoint
}

func NewValidator(d Discovery, interval time.Duration) *Validator {
	v := &Validator{
		in:            make(chan []*url.URL),
		interval:      interval,
		broadcasts:    make(chan []Endpoint),
		subscriptions: make(chan chan []Endpoint),
		subscribers:   []chan []Endpoint{},
	}
	go v.loop()
	d.Subscribe(v.in)
	return v
}

// Subscribe registers the passed channel to receive updates when the set of
// valid Endpoints changes.
func (v *Validator) Subscribe(c chan []Endpoint) {
	v.subscriptions <- c
}

func (v *Validator) loop() {
	quit := make(chan struct{}) // signal to our validation cycle
	last := []*url.URL{}        // most recent set of ideal peers

	for {
		select {
		case peers := <-v.in:
			if cmp(peers, last) {
				continue
			}

			close(quit)
			quit = make(chan struct{})
			go cycle(quit, peers, v.interval, v.broadcasts)
			last = peers

		case endpoints := <-v.broadcasts:
			go broadcastEndpoints(v.subscribers, endpoints)

		case subscriber := <-v.subscriptions:
			v.subscribers = append(v.subscribers, subscriber)
		}
	}
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

func cmp(a, b []*url.URL) bool {
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

func cycle(quit chan struct{}, peers []*url.URL, interval time.Duration, out chan []Endpoint) {
	timeout := interval / 2
	go func() { out <- pingAll(peers, timeout) }()
	t := time.Tick(interval)
	for {
		select {
		case <-quit:
			return
		case <-t:
			go func() { out <- pingAll(peers, timeout) }()
		}
	}
}

func pingAll(peers []*url.URL, timeout time.Duration) []Endpoint {
	// scatter
	chans := make([]chan Endpoint, len(peers))
	for i, peer := range peers {
		chans[i] = make(chan Endpoint)
		go func(i0 int, peer0 *url.URL) {
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

func pingOne(peer *url.URL, timeout time.Duration) Endpoint {
	e := make(chan error)
	go func() { e <- ping(peer.String()) }()

	select {
	case <-time.After(timeout):
		return Endpoint{peer, false}
	case err := <-e:
		return Endpoint{peer, err == nil}
	}
	panic("unreachable")
}

func ping(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
