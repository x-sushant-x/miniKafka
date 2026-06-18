### WAL

A Write Ahead Log System written in Golang. WAL is a foundational module where any changes to a system’s state are recorded in a persistent, append-only log before they are applied to the main data store or database.

WALs are used in:

- Databases
- OS
- Distributed Systems

#### How does it works?
At a very base level this WAL contain 2 files.
* Store
* Index

Store is responsible for storing actual data that we want to write. Index is responsible for storing a mapping of data for faster lookup.

**Memory Structure**

Store
```
[Size of Message] [Checksum] [Actual Message Data]
```
Each message stored will get an offset. Offset start from 0 and increment by one as messages get stored. Offset is important as it will be used to read message and also for indexing.

Index
```
[Message Offset] [Position In Store File]
```

Store maintain a current offset number. Whenever a message is stored, it is assigned the current offset number and after message is stored successfully current offset number is incremented by 1. 

While storing message it is first stored in store file and once store return the offset and position of the message an entry is made to index file.

While reading message we simply ask index file for the position of any message in store file. Than we can jump to that exact position in store file and read the message.


#### Segmentation
Store files can became very large if we keep appending same file. So we break large files into small manageable chunks called segments.

A segment consist of store and index module. WAL maintains a list of all the segments and a pointer to active segment.

All the writes goes to active segment and for reads WAL finds which segment have requested data and invoke read operation on particular segment.

When active segment reaches it's max file size (max store file size) a new segment is created and marked as active.


> [!WARNING]
> I built this project for learning. Do not use it for any production use.
