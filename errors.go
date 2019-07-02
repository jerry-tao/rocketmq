package rocketmq

import (
	"errors"
	"strconv"
)

var (
	// Server error
	ErrSysBusy           = errors.New("rocketmq: system busy, try again later")
	ErrSysError          = errors.New("rocketmq: system error, try again later")
	ErrTopicNotExist     = errors.New("rocketmq: topic not exist and autocreate topic is off")
	ErrTopicAlreadyExist = errors.New("rocketmq: topic already exist")

	// Client error
	ErrUpdateTopic = errors.New("rocketmq: update topic failed")
	ErrTopicInfo   = errors.New("rocketmq: topic info error")
	ErrTimeout     = errors.New("rocketmq: timeout")
	ErrConfigNil   = errors.New("rocketmq: config can't be nil")
	ErrNameSrv     = errors.New("rocketmq: namesrv can't be empty")
	ErrGroup       = errors.New("rocketmq: group can't be empty, default_consumer or default_producer")
	ErrMaxRetry    = errors.New("rocketmq: send msg reach max retry times")
)

func unknownError(code int) error {
	return errors.New("rocketmq: unknown response code " + strconv.Itoa(code))
}

func connectErr(addr string) error {
	return errors.New("rocketmq: unable to connect to: " + addr)
}
