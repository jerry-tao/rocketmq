package rocketmq

import (
	"errors"
	"sort"
	"sync"
)

type SubscriptionData struct {
	Topic           string
	SubString       string
	ClassFilterMode bool
	TagsSet         []string
	CodeSet         []string
	SubVersion      int64
}

type kv struct {
	mq *messageQueue
	pr *PullRequest
}

type Rebalance struct {
	groupName                    string
	messageModel                 string
	subscriptionInner            map[string]*SubscriptionData
	mqClient                     mqClient
	allocateMessageQueueStrategy AllocateMessageQueueStrategy
	consumer                     *DefaultConsumer
	processQueueTable            map[string]kv
	processQueueTableLock        sync.RWMutex
	mutex                        sync.Mutex
}

func NewRebalance(groupName string, client mqClient) *Rebalance {
	return &Rebalance{
		mqClient:                     client,
		groupName:                    groupName,
		subscriptionInner:            make(map[string]*SubscriptionData),
		allocateMessageQueueStrategy: new(AllocateMessageQueueAveragely),
		messageModel:                 "CLUSTERING",
		processQueueTable:            make(map[string]kv),
	}
}

func (r *Rebalance) start() {

}

func (r *Rebalance) doRebalance() {
	logger.Debug(r.mqClient.id(), ": start rebalance.")
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for topic := range r.subscriptionInner {
		r.rebalanceByTopic(topic)
	}
}

func (r *Rebalance) shutdown() {
	r.processQueueTableLock.Lock()
	defer r.processQueueTableLock.Unlock()
	for _, k := range r.processQueueTable {
		r.unlockMq(r.groupName,k.mq)
	}
}

type ConsumerIDSorter []string

func (r ConsumerIDSorter) Len() int      { return len(r) }
func (r ConsumerIDSorter) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r ConsumerIDSorter) Less(i, j int) bool {
	return r[i] < r[j]
}

type AllocateMessageQueueStrategy interface {
	allocate(consumerGroup string, currentCID string, mqAll []*messageQueue, cidAll []string) (map[string]*messageQueue, error)
}
type AllocateMessageQueueAveragely struct{}

func (r *AllocateMessageQueueAveragely) allocate(consumerGroup string, currentCID string, mqAll []*messageQueue, cidAll []string) (map[string]*messageQueue, error) {
	if currentCID == "" {
		return nil, errors.New("currentCID is empty")
	}

	if len(mqAll) == 0 {
		return nil, errors.New("mqAll is  empty")
	}

	if len(cidAll) == 0 {
		return nil, errors.New("cidAll is  empty")
	}

	result := make(map[string]*messageQueue)
	for i, cid := range cidAll {
		if cid == currentCID {
			mqLen := len(mqAll)
			cidLen := len(cidAll)
			mod := mqLen % cidLen
			var averageSize int
			if mqLen < cidLen {
				averageSize = 1
			} else {
				if mod > 0 && i < mod {
					averageSize = mqLen/cidLen + 1
				} else {
					averageSize = mqLen / cidLen
				}
			}

			var startIndex int
			if mod > 0 && i < mod {
				startIndex = i * averageSize
			} else {
				startIndex = i*averageSize + mod
			}

			var min int
			if averageSize > mqLen-startIndex {
				min = mqLen - startIndex
			} else {
				min = averageSize
			}

			for j := 0; j < min; j++ {
				result[mqAll[(startIndex+j)%mqLen].String()] = mqAll[(startIndex+j)%mqLen]
			}
			return result, nil

		}
	}

	return nil, errors.New("cant't find currentCID")
}

func (r *Rebalance) rebalanceByTopic(topic string) error {
	cidAll, err := r.mqClient.findConsumerIdList(topic, r.groupName)
	if err != nil {
		return err
	}

	info, err := r.mqClient.getTopic(topic, false)
	if err != nil {
		return err
	}
	var mqs messageQueues = info.mqs
	var consumerIdSorter ConsumerIDSorter = cidAll

	if len(mqs) > 0 && len(consumerIdSorter) > 0 {
		sort.Sort(mqs)
		sort.Sort(consumerIdSorter)
	}
	logger.Debug(r.mqClient.id(), "cids ", consumerIdSorter, "mqs", mqs)

	allocateResult, err := r.allocateMessageQueueStrategy.allocate(r.groupName, r.mqClient.id(), info.mqs, cidAll)
	logger.Debug(r.mqClient.id(), allocateResult)
	if err != nil {
		return err
	}

	r.updateProcessQueueTableInRebalance(topic, allocateResult)
	return nil
}

func (r *Rebalance) updateProcessQueueTableInRebalance(topic string, mqSet map[string]*messageQueue) {
	r.processQueueTableLock.Lock()
	defer r.processQueueTableLock.Unlock()
	for _, k := range mqSet {
		if _, ok := r.processQueueTable[k.String()]; !ok {
			if err := r.mqClient.lockMq(r.groupName, k); err == nil {
				pullRequest := new(PullRequest)
				pullRequest.consumerGroup = r.groupName
				pullRequest.messageQueue = k
				pullRequest.nextOffset = r.computePullFromWhere(k)
				pullRequest.suspend = defaultSuspend
				r.processQueueTable[k.String()] = kv{k, pullRequest}
				logger.Info(r.mqClient.id(),
					"Get lock for ", k, "start with ", pullRequest.nextOffset)
				k.lock = true
				r.consumer.pullMessageService.pullRequestQueue <- pullRequest
			} else {
				logger.Info("lock mq fail:",err," wait for next reblance.")
			}

		}
	}
	for k, v := range r.processQueueTable {
		if _, ok := mqSet[k]; !ok {
			r.unlockMq(r.groupName, v.mq)
		}
	}

}
func (r *Rebalance) unlockMq(groupName string, mq *messageQueue) {
	for k, v := range r.processQueueTable {
		if k == mq.String() {
			v.mq.lock = false
			delete(r.processQueueTable, k)
			r.mqClient.unlockMq(groupName, mq)
		}
	}
}

func (r *Rebalance) computePullFromWhere(mq *messageQueue) int64 {
	var result int64 = -1
	lastOffset := r.consumer.offsetStore.readOffset(mq, readFromStore)

	if lastOffset >= 0 {
		result = lastOffset
	} else {
		result = 0
	}
	return result
}
