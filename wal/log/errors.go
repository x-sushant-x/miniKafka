package log

import "errors"

var (
	ErrEmptyTopicName               = errors.New("empty topic name is not allowed")
	ErrStorageDirVariableNoProvided = errors.New("please make sure TOPICS_STORAGE_DIR env variable to provided")
	ErrUnableToCreateTopic          = errors.New("unable to create topic")
	ErrOffsetNotFound               = errors.New("offset not found")
)
