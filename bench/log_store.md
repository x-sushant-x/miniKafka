### Append

Initial - 

```
Operations      : 1
Elapsed         : 1.459µs
Throughput      : 685400.96 msgs/sec
Data Throughput : 669.34 MB/sec
goos: darwin
goarch: arm64
pkg: github.com/x-sushant-x/miniKafka/wal/log
cpu: Apple M1
BenchmarkAppend-8   	
Operations      : 100
Elapsed         : 242.667µs
Throughput      : 412087.35 msgs/sec
Data Throughput : 402.43 MB/sec

Operations      : 10000
Elapsed         : 27.180458ms
Throughput      : 367911.39 msgs/sec
Data Throughput : 359.29 MB/sec

Operations      : 441487
Elapsed         : 1.087514583s
Throughput      : 405959.61 msgs/sec
Data Throughput : 396.44 MB/sec
  441487	      2463 ns/op	    1156 B/op	       2 allocs/op
PASS
ok  	github.com/x-sushant-x/miniKafka/wal/log	1.897s
```

---

After Optimization -
```
Running tool: /opt/homebrew/bin/go test -test.fullpath=true -benchmem -run=^$ -bench ^BenchmarkAppend$ github.com/x-sushant-x/miniKafka/wal/log


Operations      : 1
Elapsed         : 1.834µs
Throughput      : 545256.27 msgs/sec

goos: darwin
goarch: arm64
pkg: github.com/x-sushant-x/miniKafka/wal/log
cpu: Apple M1
BenchmarkAppend-8   	
Operations      : 100
Elapsed         : 322.583µs
Throughput      : 309997.74 msgs/sec


Operations      : 10000
Elapsed         : 26.482459ms
Throughput      : 377608.44 msgs/sec


Operations      : 453127
Elapsed         : 1.01628625s
Throughput      : 445865.52 msgs/sec

  453127	      2243 ns/op	      24 B/op	       1 allocs/op
PASS
ok  	github.com/x-sushant-x/miniKafka/wal/log	1.424s
```

---

### BenchmarkReadSequential
Initial -
```
goos: darwin
goarch: arm64
pkg: github.com/x-sushant-x/miniKafka/wal/log
cpu: Apple M1
BenchmarkReadSequential-8   	  270000	      4002 ns/op	    1092 B/op	       3 allocs/op
PASS
ok  	github.com/x-sushant-x/miniKafka/wal/log	2.942s
```

After optimization - 

```
goos: darwin
goarch: arm64
pkg: github.com/x-sushant-x/miniKafka/wal/log
cpu: Apple M1
BenchmarkReadSequential-8   	  542662	      1896 ns/op	    1112 B/op	       3 allocs/op
PASS
ok  	github.com/x-sushant-x/miniKafka/wal/log	2.585s
```