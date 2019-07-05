package rocketmq

import (
	"bytes"
	"errors"
	"strconv"
)

const (
	UniqKey = "UNIQ_KEY"
)

const (
	nameValueSeparator = byte(1)
	propertySeparator  = byte(2)
)

const (
	CharacterMaxLength = 255
)

type Message struct {
	Topic      string
	Flag       int32
	Properties map[string]string
	Body       []byte
}

func NewMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		Properties: make(map[string]string),
	}
}

type MessageExt struct {
	Message
	QueueId                   int32
	StoreSize                 int32
	QueueOffset               int64
	SysFlag                   int32
	BornTimestamp             int64
	StoreTimestamp            int64
	MsgId                     string
	CommitLogOffset           int64
	BodyCRC                   int32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
}


// FIXME 返回始终为空 binary.Write不能用在[]byte类型上
func string2messageProperties(d []byte) map[string]string {
	if d == nil || len(d) <= 0 {
		return nil
	}

	rst := make(map[string]string)

	lines := bytes.Split(d, []byte{propertySeparator})
	for _, line := range lines {
		kv := bytes.Split(line, []byte{nameValueSeparator})
		if len(kv) == 2 {
			rst[string(kv[0])] = string(kv[1])
		}
	}

	return rst
}

func messageProperties2String(properties map[string]string) string {
	if properties == nil || len(properties) <= 0 {
		return ""
	}

	StringBuilder := new(bytes.Buffer)

	for k, v := range properties {
		StringBuilder.WriteString(k)
		StringBuilder.WriteByte(nameValueSeparator)
		StringBuilder.WriteString(v)
		StringBuilder.WriteByte(propertySeparator)
	}

	return StringBuilder.String()
}

func (msg Message) checkMessage(producer *DefaultProducer) (err error) {
	err = checkTopic(msg.Topic)
	if len(msg.Body) == 0 {
		err = errors.New("ResponseCode:" + strconv.Itoa(MsgIllegal) + ", the message body is null")
	}
	if len(msg.Body) > producer.conf.MaxMessageSize {
		err = errors.New("ResponseCode:" + strconv.Itoa(MsgIllegal) + ", the message body size over max value, MAX:" + strconv.Itoa(producer.conf.MaxMessageSize))
	}
	return
}

func checkTopic(topic string) (err error) {
	if topic == "" {
		err = errors.New("the specified topic is blank")
	}
	if len(topic) > CharacterMaxLength {
		err = errors.New("the specified topic is longer than topic max length 255")
	}
	if topic == defaultTopic {
		err = errors.New("the topic[" + topic + "] is conflict with default topic")
	}
	return
}

func (m *Message) SetProperty(key, value string) {
	m.Properties[key] = value
}

func (m *Message) GetProperty(key string) string {
	return m.Properties[key]
}

type SendMessageRequestHeader struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int    `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int    `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int32  `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int    `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
	MaxReconsumeTimes     int    `json:"maxReconsumeTimes"`
}

const (
	SendStatusOK = iota
	SendStatusFlushDiskTimeout
	SendStatusFlushSlaveTimeout
	SendStatusSlaveNotAvailable
)

type SendResult struct {
	SendStatus    int
	MsgId         string
	MessageQueue  *messageQueue
	QueueOffset   int64
	TransactionId string
	OffsetMsgId   string
	RegionId      string
}

func NewSendResult(sendStatus int, msgId string, offsetMsgId string, messageQueue *messageQueue, queueOffset int64) *SendResult {
	return &SendResult{
		SendStatus:   sendStatus,
		MsgId:        msgId,
		OffsetMsgId:  offsetMsgId,
		MessageQueue: messageQueue,
		QueueOffset:  queueOffset,
	}
}

type SendMessageResponseHeader struct {
	msgId         string
	queueId       int32
	queueOffset   int64
	transactionId string
}
