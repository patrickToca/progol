package progol

import (
	"net/url"
)

// StaticDiscovery is a hard-coded set of ideal peers.
type StaticDiscovery []url.URL

// Subscribe registers the passed channel to receive updates when the set of
// ideal peers changes. Since these peers are static, exactly 1 update is sent.
func (d StaticDiscovery) Subscribe(c chan []url.URL) {
	go func() { c <- ([]url.URL)(d) }()
}

// Unsubscribe is implemented only to satisfy the Discovery interface.
// For StaticDiscovery, it is a no-op.
func (d StaticDiscovery) Unsubscribe(c chan []url.URL) {
	return
}
