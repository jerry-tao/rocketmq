package rocketmq

import (
	"strconv"
	"sync"
	"time"
)

const (
	BrokerSuspendMaxTimeMillis       = 1000 * 15
	FlagCommitOffset           int32 = 0x1 << 0
	FlagSuspend                int32 = 0x1 << 1
	FlagSubscription           int32 = 0x1 << 2
	FlagClassFilter            int32 = 0x1 << 3
)

type MessageListener func(msgs []*MessageExt) error

type Consumer interface {
	Start() error
	Shutdown() bool
	RegisterMessageListener(listener MessageListener)
	Subscribe(topic string, subExpression string)
	//UnSubscribe(topic string)
}

type DefaultConsumer struct {
	conf               *Config
	consumerGroup      string
	consumeFromWhere   string
	consumerType       string
	messageModel       string
	unitMode           bool
	messageListener    MessageListener
	offsetStore        OffsetStore
	pullMessageService *PullMessageService
	rebalance          *Rebalance
	mqClient           mqClient
	running            bool
	m                  sync.Mutex
	closeCh            chan struct{}
	wg                 sync.WaitGroup
}

func NewDefaultConsumer(conf *Config) (Consumer, error) {
	err := validConfig(conf)
	if err != nil {
		return nil, err
	}

	pullMessageService := NewPullMessageService()
	mqClient := newMqClient(conf)
	rebalance := NewRebalance(conf.Group, mqClient)

	consumer := &DefaultConsumer{
		conf:               conf,
		pullMessageService: pullMessageService,
		consumerGroup:      conf.Group,
		consumeFromWhere:   "CONSUME_FROM_LAST_OFFSET",
		messageModel:       "CLUSTERING",
		offsetStore:        newOffsetStore(mqClient, conf.Group),
		rebalance:          rebalance,
		mqClient:           mqClient,
		closeCh:            make(chan struct{}),
	}

	rebalance.consumer = consumer
	pullMessageService.service = consumer
	return consumer, nil
}

func (c *DefaultConsumer) Start() error {
	c.running = true
	c.mqClient.registerConsumer(c)
	c.mqClient.start()
	go c.pullMessageService.start()
	c.rebalance.start()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		rebalanceTimer := time.NewTimer(60 * time.Second)
		rflag := true
		for rflag {
			select {
			case <-rebalanceTimer.C:
				c.doRebalance()
				rebalanceTimer.Reset(60 * time.Second)
			case <-c.closeCh:
				rflag = false
			}
		}
	}()
	return nil
}

func (c *DefaultConsumer) Shutdown() bool {
	c.m.Lock()
	defer c.m.Unlock()
	if c.running {
		//TODO unlock mq
		c.running = false
		close(c.closeCh)
		c.wg.Wait()                // 等待所有goroutine退出
		c.offsetStore.persistAll() // 最后更新offset
		c.rebalance.shutdown()
		c.mqClient.shutdown()
		return true
	}
	return false
}

func (c *DefaultConsumer) RegisterMessageListener(messageListener MessageListener) {
	c.messageListener = messageListener
}

func (c *DefaultConsumer) Subscribe(topic string, subExpression string) {
	subData := &SubscriptionData{
		Topic:     topic,
		SubString: subExpression,
	}
	c.rebalance.subscriptionInner[topic] = subData
}

func (c *DefaultConsumer) makeCallback(pullRequest *PullRequest) InvokeCallback {
	return func(responseFuture *ResponseFuture) {
		if !c.running {
			return
		}

		if responseFuture == nil || responseFuture.err != nil {

			logger.Error("pull message error,", pullRequest, responseFuture.err, ", now remove mq, wait for next reblance")
			c.offsetStore.persist(pullRequest.messageQueue, true)
			c.rebalance.unlockMq(c.consumerGroup, pullRequest.messageQueue)
			return
		}

		var nextBeginOffset = pullRequest.nextOffset

		responseCommand := responseFuture.responseCommand
		if responseCommand.Code == Success && len(responseCommand.Body) > 0 {
			pullRequest.suspend = defaultSuspend
			var err error
			pullResult, ok := responseCommand.ExtFields.(map[string]interface{})
			if ok {
				if nextBeginOffsetInter, ok := pullResult["nextBeginOffset"]; ok {
					if nextBeginOffsetStr, ok := nextBeginOffsetInter.(string); ok {
						nextBeginOffset, err = strconv.ParseInt(nextBeginOffsetStr, 10, 64)
						if err != nil {
							logger.Error("parse offset fail", err)
						}
					}
				}
			}

			msgs := responseFuture.responseCommand.decodeMessage()
			err = c.messageListener(msgs)
			if err != nil {
				logger.Error("client callback with err:", err, " wont update offset.")
			} else {
				c.offsetStore.updateOffset(pullRequest.messageQueue, nextBeginOffset, false)
			}
		} else if responseCommand.Code == PullNotFound {
		} else if responseCommand.Code == PullRetryImmediately || responseCommand.Code == PullOffsetMoved {
			logger.Error("pull message error,code=%d,request=%v", responseCommand.Code, pullRequest)
			var err error
			pullResult, ok := responseCommand.ExtFields.(map[string]interface{})
			if ok {
				if nextBeginOffsetInter, ok := pullResult["nextBeginOffset"]; ok {
					if nextBeginOffsetStr, ok := nextBeginOffsetInter.(string); ok {
						nextBeginOffset, err = strconv.ParseInt(nextBeginOffsetStr, 10, 64)
						if err != nil {
							logger.Error("parse offset fail", err)
						}
					}
				}
			}
			time.Sleep(time.Second * 1)
		} else {
			// 包括sys error和sys busy等情况 暂时移除，等待下次reblance
			logger.Error("pull message error,", pullRequest, responseFuture, unknownError(responseFuture.responseCommand.Code),", now remove mq, wait for next reblance")
			c.offsetStore.persist(pullRequest.messageQueue, true)
			c.rebalance.unlockMq(c.consumerGroup, pullRequest.messageQueue)
			return
		}

		if !pullRequest.messageQueue.lock {
			logger.Info(c.mqClient.id(), "Release lock for ", pullRequest.messageQueue, "offset with ", c.offsetStore.readOffset(pullRequest.messageQueue, readFromMemory))
			c.offsetStore.persist(pullRequest.messageQueue, true)
			c.rebalance.unlockMq(c.consumerGroup, pullRequest.messageQueue)
			return
		}
		pullRequest.nextOffset = nextBeginOffset
		c.pullMessageService.pullRequestQueue <- pullRequest
	}
}

func (c *DefaultConsumer) pullMessage(pullRequest *PullRequest) {
	if !pullRequest.messageQueue.lock {
		return
	}
	if !c.running {
		return
	}

	commitOffsetEnable := false
	commitOffsetValue := int64(0)

	commitOffsetValue = c.offsetStore.readOffset(pullRequest.messageQueue, readFromMemory)
	if commitOffsetValue > 0 {
		commitOffsetEnable = true
	}

	var sysFlag = int32(0)
	if commitOffsetEnable {
		sysFlag |= FlagCommitOffset
	}

	sysFlag |= FlagSuspend

	subscriptionData, ok := c.rebalance.subscriptionInner[pullRequest.messageQueue.topic]
	var subVersion int64
	var subString string
	if ok {
		subVersion = subscriptionData.SubVersion
		subString = subscriptionData.SubString

		sysFlag |= FlagSubscription
	}

	requestHeader := new(PullMessageRequestHeader)
	requestHeader.ConsumerGroup = pullRequest.consumerGroup
	requestHeader.Topic = pullRequest.messageQueue.topic
	requestHeader.QueueId = pullRequest.messageQueue.queueId
	requestHeader.QueueOffset = pullRequest.nextOffset

	requestHeader.SysFlag = sysFlag
	requestHeader.CommitOffset = commitOffsetValue
	requestHeader.SuspendTimeoutMillis = BrokerSuspendMaxTimeMillis
	requestHeader.MaxMsgNums = c.conf.PullMaxMsgNums

	if ok {
		requestHeader.SubVersion = subVersion
		requestHeader.Subscription = subString
	}

	c.mqClient.pullMessage(requestHeader, pullRequest, c.makeCallback(pullRequest), BrokerSuspendMaxTimeMillis)

}

func (c *DefaultConsumer) subscriptions() []*SubscriptionData {
	subscriptions := make([]*SubscriptionData, 0)
	for _, subscription := range c.rebalance.subscriptionInner {
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions
}

func (c *DefaultConsumer) doRebalance() {
	c.rebalance.doRebalance()
}
