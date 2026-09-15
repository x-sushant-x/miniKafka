# miniKafka

miniKafka is a Kafka-inspired distributed event streaming system built from scratch in Go.

The project started as a single-node implementation to understand how Kafka's core storage concepts work internally. It has since evolved into a distributed system with partitioned topics, persistent WAL storage, Raft consensus, leader election, and replicated partitions.

The goal is not to build a production-ready Kafka replacement, but to understand the engineering and distributed-systems concepts behind a log-based event streaming platform by implementing them from scratch.

---

> [!CAUTION]
> miniKafka is a learning project. It intentionally keeps many production concerns and optimizations out of scope in order to keep the implementation understandable.

### Features

- [x] Persistent message storage
- [x] Append-only log architecture
- [x] Write-Ahead Logging (WAL)
- [x] Topics
- [x] Offset-based reads
- [x] Fast index lookups
- [x] Segment rotation
- [x] CRC32 checksums
- [x] Broker recovery after restart

### Kafka-like Concepts

- [x] Broker
- [x] TCP-based broker protocol
- [x] Topics
- [x] Partitioned Topics
- [x] Offset-based consumption
- [x] Key-based partition selection

### Distributed Systems
- [x] Raft Consensus
- [x] Leader Election
- [x] Follower Replication
- [x] Raft Log Replication
- [x] Majority Based Commit
- [x] gRPC-based broker-to-broker Raft communication

May consider these:
- [ ] Zero Copy Reads
- [ ] Memory Mapping based storage

---

### Architecture
At a high level, a producer sends records to a broker. The broker routes the record to a topic partition. Each partition has its own Raft consensus group.
```text
                       +-------------+
                       |  Producer   |
                       +------+------+
                              |
                              | TCP
                              v
                       +-------------+
                       |   Broker    |
                       +------+------+
                              |
                              v
                       +-------------+
                       |    Topic    |
                       +------+------+
                              |
                              v
                       +----------------+
                       |    Partition   |
                       |   topic-0      |
                       +--------+-------+
                                |
                                v
                       +----------------+
                       |  Raft Group    |
                       |                |
                       | Leader/Follower|
                       +--------+-------+
                                |
                +---------------+---------------+
                |               |               |
                v               v               v
           +---------+     +---------+     +---------+
           | Node 1  |     | Node 2  |     | Node 3  |
           | Leader  |     |Follower |     |Follower |
           +----+----+     +----+----+     +----+----+
                |               |               |
                +---------------+---------------+
                                |
                         majority commit
                                |
                                v
                       +----------------+
                       |  Apply Channel |
                       +--------+-------+
                                |
                                v
                       +----------------+
                       | Application WAL|
                       +--------+-------+
                                |
                       +--------+--------+
                       |                 |
                       v                 v
                  +---------+       +---------+
                  |  Store  |       |  Index  |
                  +---------+       +---------+
```

Every partition owns a Write-Ahead Log (WAL). Messages are written sequentially to disk and assigned an offset. Consumers fetch messages using offsets.

---

### Storage Layout

Each topic gets its own directory.

```text
logs/
├── orders/
│   ├── 0.store // For storing actual data.
│   └── 0.index // For storing index for store file. (Offset : Position in store file)
│
├── payments/
│   ├── 0.store
│   └── 0.index
```

Raft data and state is stored separately.
```text
.raft/broker1/
└── orders-0/
    ├── log.store // Raft Log
    ├── log.index // Raft Index
    └── raft.state // Raft persistent data (votedFor & term)
```

---

### Write-Ahead Log (WAL)

The WAL is responsible for:

- Storing messages
- Assigning offsets
- Managing segments
- Recovering state after restart

Each WAL contains one or more segments.

```text
Topic
  |
  v
Partition
  |
  v
 WAL
  |
  +---- Segment 0
  |
  +---- Segment 1
  |
  +---- Segment 2
```

---

### Store Files

Store files contain the actual message data.

Example:

```text
0.store
```

Record layout:

```text
+-----------+------------+------------+------------+---------+
| Length    | Checksum   | Timestamp  | Offset     | Data    |
+-----------+------------+------------+------------+---------+
| 4 Bytes   | 4 Bytes    | 8 Bytes    | 8 Bytes    | N Bytes |
+-----------+------------+------------+------------+---------+
```

Example:

```text
[13][CRC32][timestamp][0]["Hello World"]
```

---

### Index Files

Index files provide fast offset lookups.

Example:

```text
0.index
```

Layout:

```text
+------------+-------------+
| Offset     | Position    |
+------------+-------------+
| 4 Bytes    | 8 Bytes     |
+------------+-------------+
```

Example:

```text
Offset 0 -> Position 0
Offset 1 -> Position 45
Offset 2 -> Position 91
```

---

### Broker Recovery

When the broker starts:

```text
logs/
├── orders/
├── payments/
└── users/
```

All existing topics are discovered automatically. The broker rebuilds in-memory state from disk and resumes operation.


---

### Building & Running
The current distributed implementation uses a static cluster configuration.
Example:
```json
{
    "nodes": [
        {
            "id": "1",
            "host": "127.0.0.1",
            "raft_port": "6666"
        },
        {
            "id": "2",
            "host": "127.0.0.1",
            "raft_port": "6667"
        },
        {
            "id": "3",
            "host": "127.0.0.1",
            "raft_port": "6668"
        }
    ]
}
```

Each broker also has its own configuration containing:
```text
Broker ID
Broker host
Broker port
Topic storage directory
Raft storage directory
Retention configuration
```

For the example three-node cluster:
```text
              miniKafka cluster

       +--------------------------+
       |                          |
       v                          v
   Broker 1                    Broker 2
   :5555                       :5556
   Raft :6666                  Raft :6667
       |                          |
       +------------+-------------+
                    |
                    v
                 Broker 3
                 :5557
                 Raft :6668
```

All nodes currently know the cluster membership from `cluster.json`.

Starting the Distributed Cluster
```Makefile
make build
```

The distributed branch starts a broker using its broker ID:
```
./bin/miniKafka --broker_id=1
```

```
./bin/miniKafka --broker_id=2
```

```
./bin/miniKafka --broker_id=3
```

Each broker loads:
```
config-<broker_id>.json
```

The broker first starts its Raft gRPC server, connects to its configured peers, and then starts the miniKafka broker.

Now you can use client.go file to interact with miniKafka cluster.

---
---

### Design Decisions

#### Append-Only Writes

Sequential writes are significantly faster than random writes.

Benefits:

- Better disk performance
- Simpler recovery
- Easier replication in the future

---

#### Separate Store and Index

Store:

```text
Actual message bytes
```

Index:

```text
Offset -> Position mapping
```

This enables fast reads while keeping storage efficient.

---

#### Segment-Based Storage

Benefits:

- Prevents huge log files
- Faster startup recovery
- Enables future retention policies
- Mirrors Kafka's architecture

---

#### CRC32 Checksums

Chosen because:

- Fast
- Small
- Simple
- Effective at detecting corruption

---

Open to suggestions
