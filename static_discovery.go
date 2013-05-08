package progol

import (
	"net/url"
)

// StaticDiscovery is a hard-coded set of ideal peers.
type StaticDiscovery []url.URL

// Subscribe registers the passed channel to receive updates when the set of
// ideal peers changes. Since these peers are static, exactly 1 update is sent.
func (d StaticDiscovery) Subscribe(c chan []Peer) {
	peers := make([]Peer, len(d))
	for i, _ := range d {
		peers[i] = Peer{d[i], false}
	}
	go func() { c <- peers }()
}

// Unsubscribe is implemented only to satisfy the Discovery interface.
// For StaticDiscovery, it is a no-op.
func (d StaticDiscovery) Unsubscribe(chan []Peer) {
	return
}
