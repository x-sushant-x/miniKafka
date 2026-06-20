package broker

import "errors"

var (
	ErrEmptyTopicsStorageDir = errors.New("topics storage env var: TOPICS_STORAGE_DIR is empty")
	ErrNoTopicFound          = errors.New("no topic found with given name")
)
