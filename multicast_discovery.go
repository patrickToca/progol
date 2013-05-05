package progol

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"time"
)

// MulticastDiscovery deduces ideal peers from a multicast group which all
// peers join.
type MulticastDiscovery struct {
	ttl         time.Duration
	subscribe   chan chan []url.URL
	unsubscribe chan chan []url.URL
	subscribers map[chan []url.URL]struct{}
	ids         chan multicastId
}

// NewMulticastDiscovery returns a Discovery that represents a single peer
// (myAddress, e.g. "http://1.2.3.4:8001") on a multicast group
// (multicastAddress, e.g. "udp://224.0.0.253:1357").
//
// Peers are recognized and promoted as ideal as they join the multicast group.
// Ideal peers are dropped when no heartbeat is detected in the multicast group
// for TTL (lowest resolution is 1s).
func NewMulticastDiscovery(myAddress, multicastAddress string, ttl time.Duration) (*MulticastDiscovery, error) {
	me, err := url.Parse(myAddress)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(me.Scheme, "http") {
		return nil, fmt.Errorf("myAddress must be an HTTP address")
	}

	group, err := net.ResolveUDPAddr("udp4", multicastAddress)
	if err != nil {
		return nil, err
	}

	ids := make(chan multicastId)
	go transmit(group, *me)
	go receive(group, ids)

	d := &MulticastDiscovery{
		ttl:         ttl,
		subscribe:   make(chan chan []url.URL),
		unsubscribe: make(chan chan []url.URL),
		subscribers: map[chan []url.URL]struct{}{},
		ids:         ids,
	}
	go d.loop()
	return d, nil
}

// Subscribe registers the passed channel to receive updates when the set of
// ideal peers changes.
func (d *MulticastDiscovery) Subscribe(c chan []url.URL) {
	d.subscribe <- c
}

// Subscribe unregisters the passed channel so that it will no longer receive
// updates when the set of ideal peers changes.
func (d *MulticastDiscovery) Unsubscribe(c chan []url.URL) {
	d.unsubscribe <- c
}

type multicastId struct {
	Peer string `json:"peer"`
}

func transmit(group *net.UDPAddr, me url.URL) {
	socket, err := net.DialUDP("udp4", nil, group)
	if err != nil {
		panic(err)
	}
	defer socket.Close()

	for _ = range time.Tick(1 * time.Second) {
		id := multicastId{me.String()}
		buf, err := json.Marshal(id)
		if err != nil {
			panic(err)
		}

		n, err := socket.Write(buf)
		if err != nil {
			panic(err)
		}
		if n < len(buf) {
			panic(fmt.Sprintf("broadcast: %d < %d", n, len(buf)))
		}
	}
}

func receive(group *net.UDPAddr, ids chan multicastId) {
	socket, err := net.ListenMulticastUDP("udp4", nil, group)
	if err != nil {
		panic(err)
	}
	defer socket.Close()

	socketBufferSize := 4096
	for {
		buf := make([]byte, socketBufferSize)
		n, err := socket.Read(buf)
		if err != nil {
			log.Printf("Multicast Discovery: receive: %s (exiting)", err)
			return
		}
		if n >= socketBufferSize {
			panic(fmt.Sprintf("%d >= %d", n, socketBufferSize))
		}

		var id multicastId
		if err := json.Unmarshal(buf[:n], &id); err != nil {
			continue
		}
		ids <- id
	}
}

func (d *MulticastDiscovery) loop() {
	t := time.Tick(1 * time.Second)
	m := map[string]time.Time{}
	for {
		select {
		case c := <-d.subscribe:
			if _, ok := d.subscribers[c]; !ok {
				d.subscribers[c] = struct{}{}
			}

		case c := <-d.unsubscribe:
			if _, ok := d.subscribers[c]; ok {
				delete(d.subscribers, c)
			}

		case id := <-d.ids:
			m[id.Peer] = time.Now()
			go broadcastPeers(d.subscribers, map2peers(m))

		case <-t:
			m = purge(m, d.ttl)
			go broadcastPeers(d.subscribers, map2peers(m))
		}
	}
}

func purge(m map[string]time.Time, ttl time.Duration) map[string]time.Time {
	oldest := time.Now().Add(-ttl)
	m0 := map[string]time.Time{}
	for s, t := range m {
		if t.Before(oldest) {
			continue
		}
		m0[s] = t
	}
	return m0
}

func broadcastPeers(subscribers map[chan []url.URL]struct{}, peers []url.URL) {
	for subscriber, _ := range subscribers {
		select {
		case subscriber <- peers:
			break
		case <-time.After(10 * time.Millisecond):
			panic("Multicast Discovery: uncoöperative subscriber")
		}
	}
}

func map2peers(m map[string]time.Time) []url.URL {
	peers := []url.URL{}
	for rawurl, _ := range m {
		u, err := url.Parse(rawurl)
		if err != nil {
			panic(fmt.Sprintf("Multicast Discovery: '%s': %s", rawurl, err))
		}
		peers = append(peers, *u)
	}
	return peers
}
