Raft-C - Distributed Time Series Database
==========================================

A Raft-based distributed time series database written in C, featuring
consistent hashing for sharding, a SQL-like query language, and minimal
dependencies.

This project explores distributed systems concepts with a focus on simplicity
and educational value. It implements a complete distributed time series
database with leader election, log replication, and automatic sharding.

## Features

The software evolves incrementally with the following features:

- **Raft Consensus** - [Raft algorithm](https://raft.github.io/raft.pdf) for leader election and log replication
    - UDP-based transport for efficient communication
    - Pluggable serialization (binary by default)
    - Write-Ahead Log (WAL) for durability
- **Consistent Hashing** - [Consistent hashing](https://highscalability.com/consistent-hashing-algorithm/) for sharding keys across nodes
    - Pluggable TCP/UDP transport protocol
    - Pluggable serialization (binary by default)
    - Mesh topology with all nodes connected
- **Time Series Query Language** - SQL-like query language for time series operations
    - Database and time series management
    - Flexible timestamp formats (Unix epochs, ISO dates, relative times)
    - Aggregation functions (avg, min, max, latest)
    - Time range queries with sampling intervals
- **Storage Backend** - Custom storage implementation (WIP)
- **Configuration** - Static configuration files with flag overrides (WIP)

## Architecture

The cluster is organized as **shards with replicas**:

- **Shards** - Distribute data across the cluster using consistent hashing. Each
  shard is responsible for a portion of the key space.

- **Replicas** - Each shard has multiple replicas that use Raft consensus
  to maintain consistency. One replica is the leader, others are followers.

Example topology (3 shards, 2 replicas each):
```
Shard 0: node-0 (leader) + raft-0-0, raft-0-1 (replicas)
Shard 1: node-1 (leader) + raft-1-0, raft-1-1 (replicas)
Shard 2: node-2 (leader) + raft-2-0, raft-2-1 (replicas)
```

## Building the Project

### Prerequisites

- `gcc` or `clang`
- `make`

### Build Commands

```bash
# Build everything (server, client, tests)
make

# Build specific targets
make raft-c        # Server binary
make raft-cli      # Client CLI
make raft-c-tests  # Test suite

# Clean build artifacts
make clean
```

## Quick Start

### 1. Start a Cluster

Use the convenience script to start a 3-shard cluster with 2 replicas each:

```bash
./start-cluster.sh
```

Or start nodes individually:

```bash
# Shard 0
./raft-c -c conf/node-0.conf
./raft-c -c conf/raft-0-0.conf
./raft-c -c conf/raft-0-1.conf

# Shard 1
./raft-c -c conf/node-1.conf
./raft-c -c conf/raft-1-0.conf
./raft-c -c conf/raft-1-1.conf

# Shard 2
./raft-c -c conf/node-2.conf
./raft-c -c conf/raft-2-0.conf
./raft-c -c conf/raft-2-1.conf
```

Logs are written to `logs/` directory.

### 2. Connect with the CLI Client

```bash
./raft-cli -h 127.0.0.1 -p 27778
```

### 3. Create and Query Time Series

```sql
-- Create a database
CREATEDB metrics

-- Set active database
USE metrics

-- Create a time series
CREATE cpu_usage

-- Insert data points
INSERT INTO cpu_usage VALUES (now(), 78.5)
INSERT INTO cpu_usage VALUES ('2025-01-15 12:30:00', 82.3)

-- Insert multiple points
INSERT INTO cpu_usage VALUES
    (1643673600, 78.5),
    (1643673660, 80.2),
    (1643673720, 75.1)

-- Query all values
SELECT value FROM cpu_usage

-- Query with time range
SELECT value FROM cpu_usage
BETWEEN '2025-01-01' AND '2025-01-31'

-- Aggregation queries
SELECT avg(value) FROM cpu_usage
BETWEEN '2025-01-01' AND '2025-01-31'

SELECT min(value), max(value) FROM cpu_usage

-- Downsampling with intervals
SELECT avg(value) FROM cpu_usage
BETWEEN '2025-01-01' AND '2025-01-31'
SAMPLE BY 1d

-- Limit results
SELECT value FROM cpu_usage LIMIT 100
SELECT latest(value) FROM cpu_usage

-- Meta commands
.databases
.timeseries
```

## Query Language Reference

### Supported Timestamp Formats

- **Unix epoch**: `1643673600`
- **ISO date**: `'2025-01-15 12:30:00'`
- **Relative time**: `now()`, `now() - 24h`, `now() - 7d`
- **Auto timestamp**: Omit timestamp to use current time

### Aggregate Functions

- `avg(value)` - Average value
- `min(value)` - Minimum value
- `max(value)` - Maximum value
- `latest(value)` - Most recent value

### Time Intervals

- `ms` - Milliseconds
- `s` - Seconds
- `m` - Minutes
- `h` - Hours
- `d` - Days

### Commands

- `CREATEDB <name>` - Create a new database
- `USE <name>` - Set active database
- `CREATE <timeseries>` - Create a time series
- `INSERT INTO <timeseries> VALUES (timestamp, value)` - Insert data
- `SELECT ... FROM <timeseries>` - Query data
- `DELETE <timeseries>` - Delete a time series

## Configuration

Configuration files define node behavior and cluster topology.

### Shard Node Configuration

```conf
# Cluster config
id                  0
type                shard
host                127.0.0.1:27778
shard_leaders       127.0.0.1:7778 127.0.0.1:7878 127.0.0.1:7978

# Raft replicas for this shard
raft_replicas       127.0.0.1:8778 127.0.0.1:8779 127.0.0.1:7778
raft_heartbeat_ms   150
```

### Replica Node Configuration

Similar structure but with `type replica` and appropriate ports.

## Testing

```bash
# Build and run tests
make raft-c-tests
./raft-c-tests
```

Test coverage includes (for now):
- Encoding/decoding (binary serialization)
- Statement parsing (SQL query parser)
- Time series operations (aggregations, sampling)

## Project Goals

This is a **didactic project** focused on:

- Exploring distributed systems concepts
- Keeping implementation simple and dependency-free
- Prioritizing code clarity over performance
- Using straightforward approaches (e.g., `select` for I/O multiplexing)

It is **not intended for production use**. Features are added incrementally as learning opportunities.

## Stopping the Cluster

```bash
pkill -f raft-c
```
