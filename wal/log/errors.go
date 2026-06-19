package log

import "errors"

var (
	ErrEmptyTopicName = errors.New("empty topic name is not allowed")
)
