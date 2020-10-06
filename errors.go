package kafkaavro

import (
	"errors"
	"fmt"
)

var (
	ErrPollTimeout  = errors.New("poll timeout")
	ErrPartitionEOF = errors.New("topic EOF")
)

type ErrInvalidValue struct {
	Topic string
}

func (e ErrInvalidValue) Error() string {
	return fmt.Sprintf("invalid value for topic: %s", e.Topic)
}

func IsErrInvalidValue(err error) bool {
	_, ok := err.(ErrInvalidValue)
	return ok
}

type ErrFailedCommit struct {
	Err error
}

func IsErrFailedCommit(err error) bool {
	_, ok := err.(ErrFailedCommit)
	return ok
}

func (e ErrFailedCommit) Error() string {
	return fmt.Sprintf("failed to commit message: %v", e.Err)
}

func (e ErrFailedCommit) Unwrap() error {
	return e.Err
}
