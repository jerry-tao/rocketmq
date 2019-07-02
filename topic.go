package rocketmq

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type topicInfo struct {
	topic          string
	publishInfo    *TopicPublishInfo
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
	mqs            []*messageQueue
	timestamp      int64
}

func (info *topicInfo) String() string {
	jsonByte, _ := json.Marshal(info)
	return string(jsonByte)
}

func (info *topicInfo) empty() bool {
	return len(info.QueueDatas) == 0 && len(info.BrokerDatas) == 0
}

func (info *topicInfo) expired() bool {
	return time.Now().Unix()-info.timestamp > 5*60
}

func (info *topicInfo) equal(other *topicInfo) bool {
	if other == nil {
		return false
	}
	if info.topic != other.topic {
		return false
	}
	if len(info.BrokerDatas) != len(other.BrokerDatas) {
		return false
	}
	if len(info.QueueDatas) != len(other.BrokerDatas) {
		return false
	}

	for i, bd := range info.BrokerDatas {
		if !bd.equal(other.BrokerDatas[i]) {
			return false
		}
	}
	for i, qd := range info.QueueDatas {
		if !qd.equal(other.QueueDatas[i]) {
			return false
		}
	}

	return true
}

func (info *topicInfo) buildPublishInfo() {
	info.publishInfo = newTopicPublishInfo()
	if info.OrderTopicConf != "" {
		brokers := strings.Split(info.OrderTopicConf, ";")
		for _, borker := range brokers {
			item := strings.Split(borker, ":")
			nums, _ := strconv.Atoi(item[1])
			for i := int32(0); i < int32(nums); i++ {
				mq := newMessageQueue(info.topic, item[0], i)
				info.publishInfo.messageQueueList = append(info.publishInfo.messageQueueList, mq)
			}
		}
		info.publishInfo.orderTopic = true
	} else {
		// fixme topic change mq pointer change
		qds := info.QueueDatas
		for _, qd := range qds {
			if PermName.isWritable(qd.Perm) {
				var brokerData *BrokerData
				for _, bd := range info.BrokerDatas {
					if bd.BrokerName == qd.BrokerName {
						brokerData = bd
						break
					}
				}

				if brokerData.BrokerName == "" {
					continue
				}

				if _, ok := brokerData.BrokerAddrs[strconv.Itoa(masterID)]; !ok {
					continue
				}

				for i := int32(0); i < qd.WriteQueueNums; i++ {
					mq := newMessageQueue(info.topic, qd.BrokerName, i)
					info.publishInfo.messageQueueList = append(info.publishInfo.messageQueueList, mq)
				}
			}
		}
		info.publishInfo.orderTopic = false
	}
	for _, queueData := range info.QueueDatas {
		var i int32
		for i = 0; i < queueData.ReadQueueNums; i++ {
			mq := &messageQueue{
				topic:      info.topic,
				brokerName: queueData.BrokerName,
				queueId:    i,
			}
			info.mqs = append(info.mqs, mq)
		}
	}
}

type QueueData struct {
	BrokerName     string
	ReadQueueNums  int32
	WriteQueueNums int32
	Perm           int
	TopicSynFlag   int32
}

func (q *QueueData) equal(o *QueueData) bool {
	return q.BrokerName == o.BrokerName && q.ReadQueueNums == o.ReadQueueNums && q.WriteQueueNums == o.WriteQueueNums && q.Perm == o.Perm && q.TopicSynFlag == o.TopicSynFlag
}

type BrokerData struct {
	BrokerName      string
	BrokerAddrs     map[string]string
	BrokerAddrsLock sync.RWMutex
}

func (b *BrokerData) equal(o *BrokerData) bool {
	if b.BrokerName != o.BrokerName {
		return false
	}
	for k, v := range b.BrokerAddrs {
		value, ok := o.BrokerAddrs[k]
		if !ok || value != v {
			return false
		}
	}
	return true
}

type TopicRouteData struct {
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
}

type TopicPublishInfo struct {
	orderTopic       bool
	messageQueueList []*messageQueue
}

func newTopicPublishInfo() *TopicPublishInfo {
	return &TopicPublishInfo{}
}

func (t *TopicPublishInfo) ok() (ok bool) {
	if len(t.messageQueueList) != 0 {
		ok = true
	}
	return
}

func (t *TopicPublishInfo) selectOneMessageQueue(lastBrokerName string) (messageQueue *messageQueue) {
	mqCnt := len(t.messageQueueList)
	messageQueue = t.messageQueueList[rand.Intn(mqCnt)]
	if lastBrokerName != "" {
		for i := 0; i < mqCnt; i++ {
			if lastBrokerName == t.messageQueueList[i].brokerName {
				messageQueue = t.messageQueueList[i]
				return
			}
		}
	}
	return
}
