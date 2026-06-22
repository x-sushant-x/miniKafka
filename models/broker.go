package models

type ProduceRequest struct {
	Data string `json:"data"`
}

type Request struct {
	Type   string `json:"type"`
	Topic  string `json:"topic"`
	Data   string `json:"data,omitempty"`   // For Producing
	Offset uint64 `json:"offset,omitempty"` // For Consuming
}

type Response struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Data    string `json:"data,omitempty"`
	Offset  uint64 `json:"offset,omitempty"`
}
