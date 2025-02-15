Raft-C - Raft-Based Distributed System
======

This project explores various topics in the distributed systems domain, trying
to keep it as small, simple and dependency-free as possible. Didactic in
nature, it's not concerned with the latest frills (e.g. uses `select` as the
main I/O mux) or rock solid features, improvements will come as it grows, with
no specific goals other than keeping it simple enough.

## Features

The software evolves and new features are added incrementally, at the current
state:

- [Raft Consensus](https://raft.github.io/raft.pdf) for leader election and log replication
    - UDP-Based Transport for efficient communication
    - Pluggable serialization, defaulting to binary
- [Consistent Hashing](https://highscalability.com/consistent-hashing-algorithm/) for sharding keys across nodes
    - Pluggable TCP / UDP transport protocol
    - Pluggable serialization, defaulting to binary
    - Mesh of nodes all connected, gossip protocol for larger clusters yet to be implemented
- Pluggable Storage Backend (SQLite, disk, etc.) (WIP)
- Static configuration files or override via flags (WIP)

## Building the Project

### Prerequisites

- `gcc` (or `Clang`)
- `Make`

## Play

A crude bash script, for convenience, can be used to start 3 nodes with 2 replicas
each, `start_cluster.sh`.

For the dull way, start each node with its own configuration based on its role in
the cluster (dynamic discovery to be added), e.g.

**Shard 0**

```bash
./raft-c -c conf/node-0.conf
```

**replica 0-0**

```bash
./raft-c -c conf/raft-0-0.conf
```

**replica 0-1**

```bash
./raft-c -c conf/raft-0-1.conf
```
