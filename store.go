package rocketmq

import (
	"sync"
	"sync/atomic"
)

const (
	readFromMemory = 1
	readFromStore  = 2
)

type OffsetStore interface {
	updateOffset(mq *messageQueue, offset int64, increaseOnly bool)
	readOffset(mq *messageQueue, flag int) int64
	persistAll()
	persist(mq *messageQueue, remove bool)
}

type RemoteOffsetStore struct {
	groupName   string
	mqClient    mqClient
	offsetTable sync.Map
}

func newOffsetStore(client mqClient, groupName string) OffsetStore {
	return &RemoteOffsetStore{
		mqClient:  client,
		groupName: groupName,
	}
}

func (r *RemoteOffsetStore) readOffset(mq *messageQueue, readType int) int64 {
	switch readType {
	// used for commitOffset
	case readFromMemory:
		offset, ok := r.offsetTable.Load(mq)
		if ok {
			return offset.(int64)
		}
	// used for consumeOffset
	case readFromStore:
		offset, err := r.mqClient.getOffset(r.groupName, mq)
		if err != nil {
			logger.Error("get remote offset fail:",err)
			return -1
		}
		r.updateOffset(mq, offset, false)
		return offset
	}

	return -1

}

func (r *RemoteOffsetStore) persist(mq *messageQueue, remove bool) {
	offset, ok := r.offsetTable.Load(mq)
	if ok {
		err := r.mqClient.updateOffset(r.groupName, mq, offset.(int64))
		if err != nil {
			logger.Error("persist mq offset fail:",err)
		}

		if remove {
			//r.mqClient.unlockMq(r.groupName, mq)
			r.offsetTable.Delete(mq)
		}
	}
}

func (r *RemoteOffsetStore) persistAll() {
	r.offsetTable.Range(func(key, value interface{}) bool {
		r.persist(key.(*messageQueue), true)
		return true
	})
}

type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueID       int32  `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
}

func (r *RemoteOffsetStore) updateOffset(mq *messageQueue, offset int64, increaseOnly bool) {
	if mq != nil {
		offsetOld, ok := r.offsetTable.Load(mq)
		if !ok {
			r.offsetTable.Store(mq, offset)
		} else {
			offsetValue := offsetOld.(int64)
			if increaseOnly {
				atomic.AddInt64(&offsetValue, offset)
				r.offsetTable.Store(mq, offsetValue)
			} else {
				r.offsetTable.Store(mq, offset)
			}
		}

	}

}
