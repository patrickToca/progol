# Progol

Progol is a library that enables processes to find and communicate with each
other over a network.

## Architecture

Process discovery (which are the processes that *should* be in this network?)
and validation (which of those endpoints are currently available?) are two
distinct stages.

At the lowest level, a **Discovery** component determines the set of ideal peers
from some communications substrate. For example, a static list of endpoints, or
by reading messages of a specific format from a multicast group. A couple of
Discovery mechanisms are included, but you're free to write your own.

At the next level, a **Validation** component receives the set of ideal peers
from the Discovery component, and "pings" each one to determine if it's alive.
Progol defines a ping as an HTTP GET on the process endpoint at ValidationPath,
and success as an HTTP 200 OK response.

At the top level, your application code should subscribe to updates from the
Validation component, and make whatever decisions are appropriate for the
current state of the network. Deeper introspection of peers is up to the
application to define and verify.

## Peer-to-peer vs. master coördinator

Progol enforces no architectural restrictions on process discovery. The
Multicast Discovery component that ships with Progol is an example of P2P
discovery. It's also possible to write a Discovery component that appeals
to a defined master coördinator.
