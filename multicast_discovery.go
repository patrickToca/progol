package progol

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"time"
)

type MulticastDiscovery struct {
	subscriptions chan chan []*url.URL
	subscribers   []chan []*url.URL
	ids           chan multicastId
}

func NewMulticastDiscovery(myAddress, multicastAddress string) (*MulticastDiscovery, error) {
	me, err := url.Parse(myAddress)
	if err != nil {
		return nil, err
	}

	group, err := net.ResolveUDPAddr("udp4", multicastAddress)
	if err != nil {
		return nil, err
	}

	ids := make(chan multicastId)
	go transmit(group, me)
	go receive(group, ids)

	d := &MulticastDiscovery{
		subscriptions: make(chan chan []*url.URL),
		subscribers:   []chan []*url.URL{},
		ids:           ids,
	}
	go d.loop()
	return d, nil
}

func (d *MulticastDiscovery) Subscribe(c chan []*url.URL) {
	d.subscriptions <- c
}

type multicastId struct {
	Peer string `json:"peer"`
}

func transmit(group *net.UDPAddr, me *url.URL) {
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
		case c := <-d.subscriptions:
			d.subscribers = append(d.subscribers, c)

		case id := <-d.ids:
			m[id.Peer] = time.Now()
			go broadcast(d.subscribers, map2peers(m))

		case <-t:
			m = purge(m)
			go broadcast(d.subscribers, map2peers(m))
		}
	}
}

func purge(m map[string]time.Time) map[string]time.Time {
	oldest := time.Now().Add(-3 * time.Second)
	m0 := map[string]time.Time{}
	for s, t := range m {
		if t.Before(oldest) {
			continue
		}
		m0[s] = t
	}
	return m0
}

func broadcast(subscribers []chan []*url.URL, peers []*url.URL) {
	for _, subscriber := range subscribers {
		select {
		case subscriber <- peers:
			break
		case <-time.After(10 * time.Millisecond):
			panic("Multicast Discovery: uncoöperative subscriber")
		}
	}
}

func map2peers(m map[string]time.Time) []*url.URL {
	peers := []*url.URL{}
	for rawurl, _ := range m {
		u, err := url.Parse(rawurl)
		if err != nil {
			panic(fmt.Sprintf("Multicast Discovery: '%s': %s", rawurl, err))
		}
		peers = append(peers, u)
	}
	return peers
}
