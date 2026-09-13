package raft

type LogEntry struct {
	Term    int64
	Command []byte
}

type ApplyMessage struct {
	Index   int64
	Command []byte
}
