package rocketmq

import (
	"strconv"
)

type messageQueue struct {
	topic      string
	brokerName string
	queueId    int32
	lock       bool
}

func (m *messageQueue) String() string {
	return `{"topic":"` + m.topic + `" , "brokerName":"` + m.brokerName + `", "queueId":` + strconv.Itoa(int(m.queueId)) + `}`
}

func (m *messageQueue) MarshalJSON() ([]byte, error) {
	return []byte(`{"topic":"` + m.topic + `" , "brokerName":"` + m.brokerName + `", "queueId":` + strconv.Itoa(int(m.queueId)) + `}`), nil
}

func newMessageQueue(topic string, brokerName string, queueId int32) *messageQueue {
	return &messageQueue{
		topic:      topic,
		brokerName: brokerName,
		queueId:    queueId,
	}
}


type messageQueues []*messageQueue

func (m messageQueues) Less(i, j int) bool {
	imq := m[i]
	jmq := m[j]



	if imq.topic < jmq.topic {
		return true
	} else if jmq.topic < imq.topic {
		return false
	}

	if imq.brokerName < jmq.brokerName {
		return true
	} else if jmq.brokerName < imq.brokerName {
		return false
	}

	if imq.queueId < jmq.queueId {
		return true
	}
	return false
}

func (m messageQueues) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m messageQueues) Len() int {
	return len(m)
}
