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
	groupName       string
	mqClient        mqClient
	offsetTable     map[*messageQueue]int64
	offsetTableLock sync.RWMutex
}

func newOffsetStore(client mqClient, groupName string) OffsetStore {
	return &RemoteOffsetStore{
		mqClient:    client,
		groupName:   groupName,
		offsetTable: map[*messageQueue]int64{},
	}
}

func (r *RemoteOffsetStore) readOffset(mq *messageQueue, readType int) int64 {
	switch readType {
	// used for commitOffset
	case readFromMemory:
		r.offsetTableLock.RLock()
		offset, ok := r.offsetTable[mq]
		r.offsetTableLock.RUnlock()
		if ok {
			return offset
		}
	// used for consumeOffset
	case readFromStore:
		offset, err := r.mqClient.getOffset(r.groupName, mq)
		if err != nil {
			logger.Error(err)
			return -1
		}
		r.updateOffset(mq, offset, false)
		return offset
	}

	return -1

}

func (r *RemoteOffsetStore) persist(mq *messageQueue, remove bool) {
	r.offsetTableLock.RLock()
	defer r.offsetTableLock.RUnlock()
	offset, ok := r.offsetTable[mq]
	if ok {
		err := r.mqClient.updateOffset(r.groupName, mq, offset)
		if err != nil {
			logger.Error(err)
		}

		if remove {
			delete(r.offsetTable, mq)
		}
	}
}

func (r *RemoteOffsetStore) persistAll() {
	for k := range r.offsetTable {
		r.persist(k, true)
	}
}

type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueID       int32  `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
}

func (r *RemoteOffsetStore) updateOffset(mq *messageQueue, offset int64, increaseOnly bool) {
	if mq != nil {
		r.offsetTableLock.RLock()
		offsetOld, ok := r.offsetTable[mq]
		r.offsetTableLock.RUnlock()
		if !ok {
			r.offsetTableLock.Lock()
			r.offsetTable[mq] = offset
			r.offsetTableLock.Unlock()
		} else {
			if increaseOnly {
				atomic.AddInt64(&offsetOld, offset)
				r.offsetTableLock.Lock()
				r.offsetTable[mq] = offsetOld
				r.offsetTableLock.Unlock()
			} else {
				r.offsetTableLock.Lock()
				r.offsetTable[mq] = offset
				r.offsetTableLock.Unlock()
			}
		}

	}

}
