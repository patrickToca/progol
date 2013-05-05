package main

import (
	"flag"
	"github.com/peterbourgon/progol"
	"log"
	"net/http"
	"net/url"
	"time"
)

var (
	multicastEndpoint = flag.String("multicast.address", "udp://224.0.0.253:1357", "group multicast endpoint")
	myEndpoint        = flag.String("my.address", "http://127.0.0.1:8001", "personal HTTP endpoint")
)

func init() {
	flag.Parse()
}

func main() {
	me, err := url.Parse(*myEndpoint)
	if err != nil {
		panic(err)
	}
	log.Printf("my address is: %s", me.String())

	multicast, err := url.Parse(*multicastEndpoint)
	if err != nil {
		panic(err)
	}
	log.Printf("joining multicast group on: %s", multicast.String())

	go func() {
		http.HandleFunc(progol.ValidationPath, progol.ValidationHandler)
		log.Printf("starting HTTP server on %s", me.Host)
		log.Fatal(http.ListenAndServe(me.Host, nil))
	}()

	d, err := progol.NewMulticastDiscovery(*myEndpoint, multicast.Host)
	if err != nil {
		log.Fatalf("creating Multicast Discovery: %s", err)
	}

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
