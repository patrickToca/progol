package main

import (
	"github.com/peterbourgon/progol"
	"log"
	"net/url"
	"time"
)

func main() {
	peers := []*url.URL{}
	for _, rawurl := range []string{
		"http://localhost:8001",
		"http://localhost:8002",
	} {
		peer, err := url.Parse(rawurl)
		if err != nil {
			panic(err)
		}
		peers = append(peers, peer)
	}

	d := progol.StaticDiscovery(peers)
	v := progol.NewValidator(d, 1*time.Second)

	c := make(chan []progol.Endpoint)
	go func() {
		for endpoints := range c {
			log.Printf("Endpoints: %v", endpoints)
		}
	}()
	v.Subscribe(c)

	select {}
}
