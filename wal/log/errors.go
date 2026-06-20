package log

import "errors"

var (
	ErrEmptyTopicName               = errors.New("empty topic name is not allowed")
	ErrStorageDirVariableNoProvided = errors.New("please make sure STORAGE_DIR env variable to provided")
)
