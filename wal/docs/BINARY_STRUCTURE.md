### Binary Structure

#### log.bin

```
<- LENGTH OF DATA -> <- ACTUAL DATA ->
       8 Bytes            Variable
```

#### index.bin

```
<- OFFSET -> <- POSITION IN log.bin ->
  4 Bytes            8 Bytes
```
