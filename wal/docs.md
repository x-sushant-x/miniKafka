### Files Structure

#### `Store`

Stores records in an append-only format.

```text
+-------------+---------------+-----------------+------------------+
| Length (8B) | Checksum (4B) | Timestamp (8B)  | Record Data (N)  |
+-------------+---------------+-----------------+------------------+
```

| Field      | Size     | Description                              |
|------------|----------|------------------------------------------|
| Length     | 8 Bytes  | Size of the record payload in bytes      |
| Checksum   | 4 Bytes  | CRC32 checksum of the record data        |
| Timestamp  | 8 Bytes  | Unix timestamp of record creation        |
| Data       | Variable | Actual record payload                    |

---

#### `Index`

Maintains a mapping between logical offsets and positions in `Store`.

```text
+-------------+------------------+
| Offset (4B) | Position (8B)    |
+-------------+------------------+
```

| Field    | Size    | Description                                |
|-----------|---------|--------------------------------------------|
| Offset    | 4 Bytes | Sequential record offset                   |
| Position  | 8 Bytes | Byte position of the record in `log.bin`   |

The index enables efficient offset-based lookups without scanning the entire log file.
