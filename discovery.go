package progol

import (
	"net/url"
)

// The Discovery layer discovers the set of ideal peers, and publishes that
// information as it changes.
type Discovery interface {
	Subscribe(chan []url.URL)
}
