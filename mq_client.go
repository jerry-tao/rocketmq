package rocketmq

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type mqClient interface {
	id() string // used for log
	start()     //
	shutdown()
	getTopic(topic string, autoCreate bool) (*topicInfo, error)
	sendMsg(msg *Message, communicationMode int, sendCallback SendCallback) (sendResult *SendResult, err error)
	pullMessage(header *PullMessageRequestHeader, pullRequest *PullRequest, invokeCallback InvokeCallback, timeoutMillis int64) error
	findConsumerIdList(topic string, groupName string) ([]string, error)
	unlockMq(group string, mq *messageQueue) error
	lockMq(group string, mq *messageQueue) error
	getOffset(group string, mq *messageQueue) (int64, error)
	registerConsumer(consumer *DefaultConsumer)
	updateOffset(group string, mq *messageQueue, offset int64) error
}

type defaultMqClient struct {
	conf                *Config
	clientId            string
	namesrv             string
	brokerAddrTable     map[string]map[string]string //map[brokerName]map[bokerId]addrs
	brokerAddrTableLock sync.RWMutex
	consumer            *DefaultConsumer
	topics              map[string]*topicInfo
	topicLock           sync.RWMutex
	remotingClient      RemotingClient
	running             bool
	ch                  chan struct{}
}

func newMqClient(conf *Config) mqClient {
	var id string
	if conf.InstanceName == "" {
		id = strconv.FormatInt(rand.Int63(), 10) + "@" + conf.ClientIP + "@" + strconv.Itoa(os.Getpid())
	} else {
		id = conf.InstanceName + "@" + conf.ClientIP + "@" + strconv.Itoa(os.Getpid())
	}
	return &defaultMqClient{
		namesrv:         conf.Namesrv,
		conf:            conf,
		brokerAddrTable: make(map[string]map[string]string),
		topics:          make(map[string]*topicInfo),
		clientId:        id,
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}
}

func (m *defaultMqClient) id() string {
	return m.clientId
}

func (m *defaultMqClient) start() {
	if !m.running {
		// start scan response table
		m.remotingClient.Start()
		if m.consumer != nil {
			m.startScheduledTask()
		}
		m.running = true
	}
}

func (m *defaultMqClient) shutdown() {
	close(m.ch)
	m.remotingClient.Shutdown() // 关闭连接
}

// retry 放在client里做是因为涉及了重选broker再次尝试
func (m *defaultMqClient) sendMsg(msg *Message, communicationMode int, sendCallback SendCallback) (sendResult *SendResult, err error) {
	topic, err := m.getTopic(msg.Topic, true)
	if err != nil {
		return nil, err
	}
	if topic == nil || topic.publishInfo == nil || !topic.publishInfo.ok() {
		return nil, ErrTopicInfo
	}

	var mq *messageQueue

	for times := 0; times <= m.conf.RetryTimes; times++ {
		lastBrokerName := ""
		if mq == nil {
			lastBrokerName = ""
		} else {
			lastBrokerName = mq.brokerName
		}
		mq := topic.publishInfo.selectOneMessageQueue(lastBrokerName)
		if mq != nil {
			sendResult, err = m.sendKernel(msg, mq, communicationMode, sendCallback, topic.publishInfo, times)
			if communicationMode == Sync {
				if err != nil || sendResult.SendStatus != SendStatusOK {
					continue
				}
				return sendResult, err
			}
		}
	}

	return nil, err
}

func (m *defaultMqClient) sendKernel(msg *Message, mq *messageQueue, communicationMode int, sendCallback SendCallback,
	topicPublishInfo *TopicPublishInfo, times int) (sendResult *SendResult, err error) {
	brokerAddr := m.findBrokerAddressInPublish(mq.brokerName)
	if brokerAddr == "" {
		m.getTopic(msg.Topic, true)
		brokerAddr = m.findBrokerAddressInPublish(mq.brokerName)
	}
	if brokerAddr != "" {
		requestHeader := new(SendMessageRequestHeader)
		requestHeader.ProducerGroup = m.conf.Group
		requestHeader.Topic = msg.Topic
		requestHeader.DefaultTopic = defaultTopic
		requestHeader.DefaultTopicQueueNums = m.conf.DefaultTopicQueueNums
		requestHeader.QueueId = mq.queueId
		requestHeader.SysFlag = 0
		requestHeader.Properties = messageProperties2String(msg.Properties)
		requestHeader.ReconsumeTimes = 0
		remotingCommand := buildCommand(SendMsg, requestHeader, msg.Body)
		switch communicationMode {
		case Async:
			err = m.sendMessageAsync(brokerAddr, mq.brokerName, msg, remotingCommand, sendCallback, topicPublishInfo,
				times)
		case Sync:
			sendResult, err = m.sendMessageSync(brokerAddr, mq.brokerName, msg, remotingCommand)

		}

	}
	return
}

func (m *defaultMqClient) sendMessageSync(addr string, brokerName string, msg *Message, remotingCommand *RemotingCommand) (sendResult *SendResult, err error) {
	var response *RemotingCommand
	if response, err = m.remotingClient.InvokeSync(addr, remotingCommand, m.conf.TimeoutMillis); err != nil {
		return nil, err
	}
	return m.processSendResponse(brokerName, msg, response)
}

func (m *defaultMqClient) sendMessageAsync(addr string, brokerName string, msg *Message,
	remotingCommand *RemotingCommand, sendCallback SendCallback, topicPublishInfo *TopicPublishInfo, times int) (err error) {
	invokeCallback := func(responseFuture *ResponseFuture) {
		var sendResult *SendResult
		responseCommand := responseFuture.responseCommand
		if responseFuture.err != nil && times == m.conf.RetryTimes {
			sendCallback(nil, responseFuture.err)
			return
		}
		sendResult, err = m.processSendResponse(brokerName, msg, responseCommand)
		if err == nil && sendResult.SendStatus == SendStatusOK {
			sendCallback(sendResult, err)
			return
		}
		if times == m.conf.RetryTimes {
			sendCallback(nil, ErrMaxRetry)
		}

	}
	err = m.remotingClient.InvokeAsync(addr, remotingCommand, m.conf.TimeoutMillis, invokeCallback)
	return
}

func (d *defaultMqClient) processSendResponse(brokerName string, msg *Message, response *RemotingCommand) (sendResult *SendResult, err error) {
	var sendStatus int

	if response == nil {
		err = errors.New("response in processSendResponse is nil")
		return
	}
	switch response.Code {
	case FlushDiskTimeout:
		sendStatus = SendStatusFlushDiskTimeout
	case FlushSlaveTimeout:
		sendStatus = SendStatusFlushSlaveTimeout
	case SlaveNotAvailable:
		sendStatus = SendStatusSlaveNotAvailable
	case SysBusy:
		err = ErrSysBusy
	case SysError:
		err = ErrSysError
	case TopicNotExist:
		err = ErrTopicNotExist
	case Success:
		sendStatus = SendStatusOK
		responseHeader := response.decodeCommandCustomHeader()
		messageQueue := newMessageQueue(msg.Topic, brokerName, responseHeader.queueId)

		sendResult = NewSendResult(sendStatus, msg.GetProperty(UniqKey), responseHeader.msgId, messageQueue, responseHeader.queueOffset)
		sendResult.TransactionId = responseHeader.transactionId
		return
	default:
		err = unknownError(response.Code)
	}
	return
}

func (m *defaultMqClient) findBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) (brokerAddr string, slave bool, found bool) {
	slave = false
	found = false
	m.brokerAddrTableLock.RLock()
	brokerMap, ok := m.brokerAddrTable[brokerName]
	m.brokerAddrTableLock.RUnlock()
	if ok {
		brokerAddr, ok = brokerMap[strconv.FormatInt(brokerId, 10)]
		slave = brokerId != 0
		found = ok

		if !found && !onlyThisBroker {
			var id string
			for id, brokerAddr = range brokerMap {
				slave = id != "0"
				found = true
				break
			}
		}
	} else {

	}

	return
}

func (m *defaultMqClient) findBrokerAddrByTopic(topic string) (addr string, ok bool) {
	m.topicLock.RLock()
	topicRouteData, ok := m.topics[topic]
	m.topicLock.RUnlock()
	if !ok {
		return "", ok
	}

	brokers := topicRouteData.BrokerDatas
	if len(brokers) > 0 {
		brokerData := brokers[0]
		if ok {
			brokerData.BrokerAddrsLock.RLock()
			addr, ok = brokerData.BrokerAddrs["0"]
			brokerData.BrokerAddrsLock.RUnlock()

			if ok {
				return
			}
			for _, addr = range brokerData.BrokerAddrs {
				return addr, ok
			}
		}
	}
	return
}

func (m *defaultMqClient) findConsumerIdList(topic string, groupName string) ([]string, error) {
	brokerAddr, ok := m.findBrokerAddrByTopic(topic)
	if !ok {
		_, err := m.getTopic(topic, false)
		if err != nil {
			logger.Error(err)
		}
		brokerAddr, ok = m.findBrokerAddrByTopic(topic)
	}

	if ok {
		return m.findConsumerIdListKernel(brokerAddr, groupName, 3000)
	}

	return nil, errors.New("can't find broker")

}

func (m *defaultMqClient) findConsumerIdListKernel(addr string, consumerGroup string, timeoutMillis int64) ([]string, error) {
	requestHeader := new(GetConsumerListByGroupRequestHeader)
	requestHeader.ConsumerGroup = consumerGroup
	request := buildCommand(GetConsumerListByGroup, requestHeader, nil)

	response, err := m.remotingClient.InvokeSync(addr, request, timeoutMillis)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	if response.Code == Success {
		getConsumerListByGroupResponseBody := new(GetConsumerListByGroupResponseBody)
		bodyjson := strings.Replace(string(response.Body), "0:", "\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "1:", "\"1\":", -1)
		err := json.Unmarshal([]byte(bodyjson), getConsumerListByGroupResponseBody)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		return getConsumerListByGroupResponseBody.ConsumerIDList, nil
	}

	return nil, errors.New("getConsumerIdListByGroup error")
}

func (m *defaultMqClient) updateTopicRouteInfoFromNameServer() {
	for topic := range m.topics {
		m.getTopic(topic, false)
	}
}

func (m *defaultMqClient) getTopic(topicName string, autoCreate bool) (topic *topicInfo, err error) {
	m.topicLock.RLock()
	topic, ok := m.topics[topicName]
	m.topicLock.RUnlock()
	if ok && !topic.empty() && !topic.expired() {
		return
	}
	topic, err = m.getTopicKernel(topicName, 1000*1000)
	if err != nil {
		if err == ErrTopicNotExist && autoCreate {
			topic, err = m.getTopicKernel(defaultTopic, 1000*1000)
			if err != nil {
				return
			}
			for _, data := range topic.QueueDatas {
				queueNums := int32(math.Min(float64(defaultTopicQueueNums), float64(data.ReadQueueNums)))
				data.ReadQueueNums = queueNums
				data.WriteQueueNums = queueNums
			}
		}
	}

	if topic != nil && !topic.empty() {
		m.topicLock.RLock()
		old := m.topics[topicName]
		m.topicLock.RUnlock()
		if !topic.equal(old) {
			m.brokerAddrTableLock.Lock()
			for _, bd := range topic.BrokerDatas {
				m.brokerAddrTable[bd.BrokerName] = bd.BrokerAddrs
			}
			m.brokerAddrTableLock.Unlock()
			m.topicLock.Lock()
			m.topics[topicName] = topic
			m.topicLock.Unlock()
		}
		return topic, nil
	}
	return nil, ErrUpdateTopic
}

func (m *defaultMqClient) getTopicKernel(topic string, timeoutMillis int64) (*topicInfo, error) {
	requestHeader := &GetRouteInfoRequestHeader{
		topic: topic,
	}

	remotingCommand := buildCommand(GetRouteinfoByTopic, requestHeader, nil)

	response, err := m.remotingClient.InvokeSync(m.namesrv, remotingCommand, timeoutMillis)
	if err != nil {
		return nil, err
	}
	switch response.Code {
	case Success:
		info := new(topicInfo)
		bodyjson := strings.Replace(string(response.Body), ",0:", ",\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1)
		bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
		err = json.Unmarshal([]byte(bodyjson), info)
		info.timestamp = time.Now().Unix()
		info.topic = topic
		info.buildPublishInfo()
		if err != nil {
			logger.Error("json.Unmarshal", err)
			return nil, err
		}
		return info, nil
	case TopicNotExist:
		return nil, ErrTopicNotExist
	case TopicExistAlready:
		return nil, ErrTopicAlreadyExist
	default:
		return nil, unknownError(response.Code)
	}
}

func (m *defaultMqClient) prepareHeartbeatData() *HeartbeatData {
	heartbeatData := new(HeartbeatData)
	heartbeatData.ClientId = m.clientId
	heartbeatData.ConsumerDataSet = make([]*ConsumerData, 0)
	if m.consumer != nil {
		consumerData := new(ConsumerData)
		consumerData.GroupName = m.consumer.consumerGroup
		consumerData.ConsumerType = m.consumer.consumerType
		consumerData.ConsumeFromWhere = m.consumer.consumeFromWhere
		consumerData.MessageModel = m.consumer.messageModel
		consumerData.SubscriptionDataSet = m.consumer.subscriptions()
		consumerData.UnitMode = m.consumer.unitMode

		heartbeatData.ConsumerDataSet = append(heartbeatData.ConsumerDataSet, consumerData)
	}
	return heartbeatData
}

func (m *defaultMqClient) unlockMq(groupName string, mq *messageQueue) error {
	lock := new(LockBatchRequest)
	lock.ConsumerGroup = groupName
	lock.ClientId = m.clientId
	lock.MqSet = append(lock.MqSet, mq)

	m.brokerAddrTableLock.RLock()
	brokerAddr, _, found := m.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	m.brokerAddrTableLock.RUnlock()

	if found {
		data, err := json.Marshal(*lock)
		remotingCommand := buildCommand(UNLockBatchMq, nil, data)
		response, err := m.remotingClient.InvokeSync(brokerAddr, remotingCommand, 3000)
		if err != nil {
			logger.Error(err)
		} else {
			if response == nil || response.Code != Success {
				logger.Error(m.id(), "unlock failed")
			}
		}
	}
	return nil
}

func (m *defaultMqClient) lockMq(groupName string, mq *messageQueue) error {
	lock := new(LockBatchRequest)
	lock.ConsumerGroup = groupName
	lock.ClientId = m.clientId
	lock.MqSet = append(lock.MqSet, mq)

	m.brokerAddrTableLock.RLock()
	brokerAddr, _, found := m.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	m.brokerAddrTableLock.RUnlock()

	if found {

		data, err := json.Marshal(*lock)
		if err != nil {
			logger.Error(err)
			return err
		}
		remotingCommand := buildCommand(LockBatchMq, nil, data)
		logger.Debug("try lock mq ["+mq.topic, mq.queueId, "] to broker[", brokerAddr+"]")
		response, err := m.remotingClient.InvokeSync(brokerAddr, remotingCommand, 3000)
		if err != nil {
			logger.Error(err)
			return err
		} else {
			var result map[string][]interface{}

			json.Unmarshal(response.Body, &result)
			if response == nil || response.Code != Success || len(result["lockOKMQSet"]) == 0 {
				return errors.New("lock fail")
			} else {
				return nil
			}
		}
	}
	return errors.New("can't find mq")

}

func (m *defaultMqClient) sendHeartbeatToAllBrokerWithLock() error {
	logger.Debug("start heartbeat")
	heartbeatData := m.prepareHeartbeatData()
	if len(heartbeatData.ConsumerDataSet) == 0 {
		logger.Debug("conumer nil")
		return errors.New("send heartbeat error")
	}

	m.brokerAddrTableLock.RLock()
	for _, brokerTable := range m.brokerAddrTable {
		for brokerId, addr := range brokerTable {
			if addr == "" || brokerId != "0" {
				continue
			}

			data, err := json.Marshal(*heartbeatData)
			if err != nil {
				logger.Error(err)
				return err
			}
			remotingCommand := buildCommand(HeartBeat, nil, data)
			response, err := m.remotingClient.InvokeSync(addr, remotingCommand, 3000)
			if err != nil {
				logger.Error(err)
			} else {
				if response == nil || response.Code != Success {
					logger.Error("send heartbeat response  error")
				}
			}
		}
	}
	m.brokerAddrTableLock.RUnlock()
	return nil
}

func (m *defaultMqClient) startScheduledTask() {
	m.updateTopicRouteInfoFromNameServer()
	// 定时更新topic
	go func() {

		updateTopicRouteTimer := time.NewTimer(5 * time.Second)
		uflag := true
		for uflag {
			select {
			case <-updateTopicRouteTimer.C:
				m.updateTopicRouteInfoFromNameServer()
				updateTopicRouteTimer.Reset(5 * time.Second)
			case <-m.ch:
				uflag = false
			}

		}
	}()

	// 心跳保持
	go func() {
		heartbeatTimer := time.NewTimer(10 * time.Second)
		m.sendHeartbeatToAllBrokerWithLock()
		hflag := true
		for hflag {
			select {
			case <-heartbeatTimer.C:
				m.sendHeartbeatToAllBrokerWithLock()
				heartbeatTimer.Reset(5 * time.Second)
			case <-m.ch:
				hflag = false
			}

		}
	}()

}

func (m *defaultMqClient) getOffset(groupName string, mq *messageQueue) (int64, error) {
	brokerAddr, _, found := m.findBrokerAddressInSubscribe(mq.brokerName, 0, false)

	if !found {
		if _, err := m.getTopic(mq.topic, false); err != nil {
			return 0, err
		}
		brokerAddr, _, found = m.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	}

	if found {
		requestHeader := &QueryConsumerOffsetRequestHeader{}
		requestHeader.Topic = mq.topic
		requestHeader.QueueId = mq.queueId
		requestHeader.ConsumerGroup = groupName
		return m.getOffsetKernel(brokerAddr, requestHeader, 3000)
	}

	return 0, errors.New("fetch consumer offset error")
}

func (m *defaultMqClient) getOffsetKernel(addr string, requestHeader *QueryConsumerOffsetRequestHeader, timeoutMillis int64) (int64, error) {

	remotingCommand := buildCommand(QueryConsumerOffset, requestHeader, nil)

	response, err := m.remotingClient.InvokeSync(addr, remotingCommand, timeoutMillis)
	if err != nil {
		logger.Error(err)
		return 0, err
	}
	if response.Code == QueryNotFound {
		return 0, nil
	}

	if extFields, ok := (response.ExtFields).(map[string]interface{}); ok {
		if offsetInter, ok := extFields["offset"]; ok {
			if offsetStr, ok := offsetInter.(string); ok {
				offset, err := strconv.ParseInt(offsetStr, 10, 64)
				if err != nil {
					logger.Error(err)
					return 0, err
				}
				return offset, nil

			}
		}
	}
	logger.Error(requestHeader, response)
	return 0, errors.New("query offset error")
}

func (m *defaultMqClient) updateConsumerOffsetOneway(addr string, header *UpdateConsumerOffsetRequestHeader, timeoutMillis int64) error {
	remotingCommand := buildCommand(UpdateCconsumerOffset, header, nil)
	res, err := m.remotingClient.InvokeSync(addr, remotingCommand, timeoutMillis)
	logger.Debug(res)
	return err

}

func (m *defaultMqClient) findBrokerAddressInPublish(brokerName string) string {
	tmpMap := m.brokerAddrTable[brokerName]
	if len(tmpMap) != 0 {
		brokerName = tmpMap[strconv.Itoa(masterID)]
	}
	return brokerName
}

func (m *defaultMqClient) registerConsumer(consumer *DefaultConsumer) {
	m.consumer = consumer
	for _, sub := range m.consumer.subscriptions() {
		m.topics[sub.Topic] = &topicInfo{topic: sub.Topic}
	}
	m.remotingClient.RegisterResponse(NotifyConsumerIdsChanged, func(future *ResponseFuture) { consumer.doRebalance() })
}

func (m *defaultMqClient) pullMessageKernel(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	return m.remotingClient.InvokeAsync(addr, request, timeoutMillis, invokeCallback)
}

func (m *defaultMqClient) pullMessage(header *PullMessageRequestHeader, pullRequest *PullRequest, invokeCallback InvokeCallback, timeoutMillis int64) error {
	brokerAddr, _, found := m.findBrokerAddressInSubscribe(pullRequest.messageQueue.brokerName, 0, false)

	if found {
		remotingCommand := buildCommand(PullMsg, header, nil)
		return m.pullMessageKernel(brokerAddr, remotingCommand, 1000, invokeCallback)
	}
	return errors.New("cant find broker")
}

func (m *defaultMqClient) updateOffset(groupName string, mq *messageQueue, offset int64) error {
	addr, _, found := m.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	if !found {
		if _, err := m.getTopic(mq.topic, false); err != nil {
			return err
		}
		addr, _, found = m.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	}

	if found {
		requestHeader := &UpdateConsumerOffsetRequestHeader{
			ConsumerGroup: groupName,
			Topic:         mq.topic,
			QueueID:       mq.queueId,
			CommitOffset:  offset,
		}

		m.updateConsumerOffsetOneway(addr, requestHeader, 5*1000)
		return nil
	}
	return errors.New("not found broker")
}
