package models

type Record struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Offset    uint64
}
