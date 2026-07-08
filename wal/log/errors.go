package log

import "errors"

var (
	ErrEmptyTopicName               = errors.New("empty topic name is not allowed")
	ErrStorageDirVariableNoProvided = errors.New("please make sure topics_storage_dir config variable to provided")
	ErrUnableToCreateTopic          = errors.New("unable to create topic")
	ErrOffsetNotFound               = errors.New("offset not found")
	ErrPartitionNotFound            = errors.New("partition not found")
	ErrPartitionCantBeZero          = errors.New("partitions can't be zero")
	ErrTopicAlreadyExists           = errors.New("topic already exists")
)
