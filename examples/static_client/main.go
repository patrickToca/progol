package main

import (
	"flag"
	"fmt"
	"github.com/peterbourgon/progol"
	"log"
	"net/http"
	"net/url"
	"time"
)

var (
	port = flag.Int("port", 8001, "listening port")
)

func init() {
	flag.Parse()
}

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

	go func() {
		http.HandleFunc(progol.ValidationPath, progol.ValidationHandler)
		log.Printf("listening on :%d", *port)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
	}()

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
