package broker

import "errors"

var (
	ErrEmptyTopicsStorageDir = errors.New("topics storage config variable: topics_storage_dir is empty or either not specified")
	ErrNoTopicFound          = errors.New("no topic found with given name")
	ErrTopicAlreadyExists    = errors.New("topic already exist")
)
