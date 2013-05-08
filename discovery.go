package progol

// The Discovery layer discovers the set of ideal peers, and publishes that
// information as it changes. Ideal peers always have OK=false (unknown).
type Discovery interface {
	Subscribe(chan []Peer)
	Unsubscribe(chan []Peer)
}
