package main

import (
	"flag"
	"github.com/peterbourgon/progol"
	"log"
	"net/http"
	"sync"
	"time"
)

var (
	group = flag.String("group", "", "group (multicast) discovery address")
	me    = flag.String("me", "", "my (http) address")
)

func init() {
	flag.Parse()
}

func main() {
	d := MustNewMulticastDiscovery(*me, *group, 1*time.Second)
	v := progol.NewValidator(d, 1*time.Second)

	r := progol.Registry{}
	r.Add(progol.State{"red", resetToRed, upgradeToGreen})
	r.Add(progol.State{"yellow", resetToRed, upgradeToGreen})
	r.Add(progol.State{"green", resetToRed, stayInGreen})

	c := make(chan progol.StateChange)
	go application(c)

	m := progol.NewMachine(r, "red", v, c)

	http.HandleFunc(progol.ValidationPath, progol.ValidationHandler)
	http.HandleFunc(progol.ProposalPath, progol.ProposalHandler(m.AcceptProposal()))
	go log.Fatal(http.ListenAndServe(*me, nil))

	select {}
}

type Application struct {
	sync.Mutex
	State         string
	Endpoints     []Endpoint
	RequiredPeers int
}

// Hmm.
// How can our OnXxx functions return the next state without consulting
// state themselves? The relationship of our Machine appears inverted.

func (a *Application) OnEndpoints(endpoints []Endpoint) (string, error) {
	if progol.CmpEndpoints(endpoints, a.Endpoints) {
		return "", nil // same as before = no change
	}

	// any other change in Endpoints requires a recalculation
	a.Endpoints = endpoints
	a.State = "yellow"
	return "yellow", nil
}

func MustNewMulticastDiscovery(myAddr, groupAddr string, ttl time.Duration) *progol.MulticastDiscovery {
	d, err := progol.NewMulticastDiscovery(*me, *group, ttl)
	if err != nil {
		panic(err)
	}
	return d
}
