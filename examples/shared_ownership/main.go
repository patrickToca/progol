package main

import (
	"bytes"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/peterbourgon/progol"
	"github.com/peterbourgon/raft"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	datafile           = flag.String("datafile", "data.txt", "shared data file")
	maxmem             = flag.Int64("maxmem", 1024, "max memory for this process (bytes)")
	host               = flag.String("host", "127.0.0.1", "binding host")
	discoveryGroup     = flag.String("discovery.group", "224.0.0.253:1357", "multicast group for discovery")
	discoveryLifetime  = flag.Duration("discovery.lifetime", 5*time.Second, "peer assumed shutdown if no successful ping for this long")
	validationInterval = flag.Duration("validation.interval", 1*time.Second, "ping interval for peers")
	minimum            = flag.Int("minimum", 3, "minimum number of peers in network before we start negotiating")
)

func init() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
}

func main() {
	// Gather statistics about the data file.
	filesz, checksum, err := stat(*datafile)
	if err != nil {
		log.Fatal(err)
	}

	// Set up this process to be discovered. Choose a random port.
	discovery, port, err := newMulticastDiscovery(*host, *discoveryGroup, *discoveryLifetime)
	if err != nil {
		log.Fatal(err)
	}

	// Build a state machine arount the data file statistics. ID is the port.
	machine, err := newStateMachine(uint64(port), filesz, checksum, *maxmem)
	if err != nil {
		log.Fatal(err)
	}

	// Build a Raft server, applying into our state machine. ID is the port.
	raftServer := raft.NewServer(uint64(port), &bytes.Buffer{}, machine.Apply)

	// HTTP server, for Progol discovery/validation, and Raft communication.
	http.HandleFunc(progol.ValidationPath, okHandler)
	raft.HTTPTransport(http.DefaultServeMux, raftServer)
	go http.ListenAndServe(net.JoinHostPort(*host, fmt.Sprint(port)), nil)

	// When we discover new peers at the Progol level,
	// we'll want to propagate that topology to Raft.
	substrate := make(chan []progol.Peer)
	ready := make(chan struct{})
	go propagate(substrate, raftServer, *minimum, ready)

	// Start a validator, to publish Progol peers to our propagator.
	validator := progol.NewValidator(discovery, *validationInterval)
	validator.Subscribe(substrate)
	defer validator.Unsubscribe(substrate)

	// Wait for some minimum number of nodes.
	<-ready

	// Then, start the Raft server.
	log.SetOutput(&bytes.Buffer{})
	raftServer.Start()

	// After some random amount of time, make a claim
	time.Sleep(time.Duration(1000+rand.Intn(1000)) * time.Millisecond)
	raftServer.Command(machine.MakeClaim(), make(chan []byte, 1))

	// Wait until our state machine reports the offset we own.
	offset, length := machine.Segment() // blocking

	// Process that segment
	output := process(*datafile, offset, length)
	fmt.Printf("%s\n", output)

	<-signalHandler()
}

// newMulticastDiscovery creates a progol.MulticastDiscovery for this process,
// choosing a random port.
func newMulticastDiscovery(host, group string, ttl time.Duration) (progol.Discovery, int, error) {
	for i := 0; i < 10; i++ {
		port := 10000 + rand.Intn(9999)
		laddr := "http://" + net.JoinHostPort(host, fmt.Sprint(port))
		discovery, err := progol.NewMulticastDiscovery(laddr, group, ttl)
		if err != nil {
			log.Printf("%s: %s, retrying...", laddr, err)
			time.Sleep(time.Duration(10+rand.Intn(40)) * time.Millisecond)
			continue
		}
		log.Printf("Multicast Discovery on %s", laddr)
		return discovery, port, nil
	}
	return nil, 0, fmt.Errorf("couldn't find free port")
}

func stat(filename string) (int64, string, error) {
	f, err := os.Open(*datafile)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, "", err
	}
	filesz := fi.Size()

	h := md5.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	} else if n != filesz {
		return 0, "", fmt.Errorf("bad read (%d != %d)", n, filesz)
	}
	checksum := fmt.Sprintf("%x", h.Sum(nil))

	return filesz, checksum, nil
}

func okHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte{})
}

// propagate listens for changes in the set of Progol-validated peers, and
// propagates those changes to the Raft server.
func propagate(substrate chan []progol.Peer, raftServer *raft.Server, minimum int, ready chan struct{}) {
	signaled := false
	for basePeers := range substrate {
		log.Printf("Propagate: got %d base peer(s)", len(basePeers))
		raftPeers := []raft.Peer{}
		for _, basePeer := range basePeers {
			raftPeer, err := raft.NewHTTPPeer(basePeer.URL)
			if err != nil {
				log.Printf("Propagate: %s (OK=%v): %s", basePeer.URL, basePeer.OK, err)
				continue
			}
			log.Printf("Propagate: %s is a valid Raft peer", basePeer.URL.String())
			raftPeers = append(raftPeers, raftPeer)
		}
		if n := len(raftPeers); n > 0 {
			if err := raftServer.SetConfiguration(raftPeers...); err != nil {
				log.Printf("Propagate: SetConfiguration (%d): %s", n, err)
			}
			log.Printf("Propagate: SetConfiguration (%d) successful", n)
			if n >= minimum && !signaled {
				close(ready)
				signaled = true
			}
		}
	}
}

func process(filename string, offset, length int64) string {
	f, err := os.Open(filename)
	if err != nil {
		return err.Error()
	}
	defer f.Close()

	if _, err := f.Seek(offset, 0); err != nil {
		return err.Error()
	}

	buf := make([]byte, length)
	n, err := f.Read(buf)
	if err != nil {
		return err.Error()
	} else if int64(n) != length {
		return fmt.Sprintf("short read (%d < %d)", n, length)
	}

	spaces, periods := 0, 0
	for _, c := range buf {
		switch c {
		case ' ':
			spaces++
		case '.':
			periods++
		}
	}
	return fmt.Sprintf("There were %d space(s) and %d period(s)", spaces, periods)
}

func signalHandler() chan os.Signal {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT)
	return c
}
