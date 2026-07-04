package models

type ProduceRequest struct {
	Data string `json:"data"`
}

// TODO - We might use different structs for different kind of request.
type Request struct {
	Type            string `json:"type"`
	Topic           string `json:"topic"`
	Data            string `json:"data,omitempty"` // For Producing
	Key             string `json:"key,omitempty"`
	Offset          uint64 `json:"offset,omitempty"`           // For Consuming
	Partition       int    `json:"parition,omitempty"`         // For Consuming
	TotalPartitions int    `json:"total_partitions,omitempty"` // For creating topic
}

type Response struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Data    string `json:"data,omitempty"`
	Offset  uint64 `json:"offset,omitempty"`
}
