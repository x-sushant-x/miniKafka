# miniKafka

A Kafka-inspired message broker written in Go that implements the core building blocks of a distributed log system, including:

> miniKafka is a learning project built to understand how Apache Kafka works internally by implementing its fundamental concepts from scratch.

---

### Features

✅ Persistent message storage

✅ Append-only log architecture

✅ Write-Ahead Logging (WAL)

✅ Topics

✅ Offset-based reads

✅ Fast index lookups

✅ Segment rotation

✅ Persistent storage

✅ Broker recovery after restart

- [ ] TCP-based broker protocol

- [ ] Replication

- [ ] Log Retention

- [ ] Log Compaction

- [ ] Partitioned Topics

- [ ] Zero Copy Reads

- [ ] Time-Based Indexes

- [ ] Zero-Copy Reads

- [ ] Raft Consensus

- [ ] Leader / Follower Replication

---

### Architecture

```text
                 +-------------+
                 |  Producer   |
                 +------+------+
                        v HTTP
                 +-------------+
                 |   Broker    |
                 +------+------+
                        v
                 +-------------+
                 |    Topic    |
                 +------+------+
                        v
                 +-------------+
                 |     WAL     |
                 +------+------+
                        |
           +------------+------------+
           v                         v
     +-----------+             +-----------+
     |   Store   |             |   Index   |
     +-----------+             +-----------+
```

Every topic owns a Write-Ahead Log (WAL). Messages are written sequentially to disk and assigned an offset. Consumers fetch messages using offsets.

---

### Storage Layout

Each topic gets its own directory.

```text
logs/
├── orders/
│   ├── 0.store
│   └── 0.index
│
├── payments/
│   ├── 0.store
│   └── 0.index
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

### Current Limitations

Compared to Apache Kafka, miniKafka currently does not support:

- Partitions
- Consumer Groups
- Replication
- Leader Election
- TCP Binary Protocol
- Log Compaction
- Retention Policies
- Compression
- Batching
- Time Indexes
- ISR Replicas
- Communication is currently performed over HTTP.
