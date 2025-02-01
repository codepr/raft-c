Raft-C
======

UDP based single file implementation of the [consensus algorithm RAFT](https://raft.github.io/raft.pdf).
Protoype of a barebones TCP server accepting integers as values to be
replicated inside the system, features:

- Cluster management
    - Seed nodes currently set at 3 to boot up the cluster
    - Dynamic node joining and leaving the cluster
- State replication
    - Heartbeats
    - Resilient to nodes dropping out of the cluster
    - State re-applied to the new joining node

## Play

Assumptions for simplicity
- 3 seed nodes (can also be launched just a single seed node)
- `localhost` as host (only specifying the `-p` port for the time being)

**Terminal # 1**

```bash
./raft-c -n 0
```

**Terminal # 2**

```bash
./raft-c -n 1
```

**Terminal # 3**

```bash
./raft-c -n 2
```

**Terminal # 4**

```bash
./raft-c -p 8999
```
