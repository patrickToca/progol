package progol

import (
	"net/url"
)

// StaticDiscovery is a hard-coded set of ideal peers.
type StaticDiscovery []*url.URL

// Subscribe sends the set of predefined ideal peers along the passed chan
// exactly once.
func (d StaticDiscovery) Subscribe(c chan []*url.URL) {
	go func() { c <- ([]*url.URL)(d) }()
}
