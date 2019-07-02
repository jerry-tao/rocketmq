package rocketmq

import (
	"bytes"
	"encoding/binary"
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

func decodeMessage(data []byte) []*MessageExt {
	buf := bytes.NewBuffer(data)
	var storeSize, magicCode, bodyCRC, queueId, flag, sysFlag, reconsumeTimes, bodyLength, bornPort, storePort int32
	var queueOffset, physicOffset, preparedTransactionOffset, bornTimeStamp, storeTimestamp int64
	var topicLen byte
	var topic, body, properties, bornHost, storeHost []byte
	var propertiesLength int16

	// var propertiesMap map[string]string

	msgs := make([]*MessageExt, 0, 32)
	for buf.Len() > 0 {
		msg := new(MessageExt)
		binary.Read(buf, binary.BigEndian, &storeSize)
		binary.Read(buf, binary.BigEndian, &magicCode)
		binary.Read(buf, binary.BigEndian, &bodyCRC)
		binary.Read(buf, binary.BigEndian, &queueId)
		binary.Read(buf, binary.BigEndian, &flag)
		binary.Read(buf, binary.BigEndian, &queueOffset)
		binary.Read(buf, binary.BigEndian, &physicOffset)
		binary.Read(buf, binary.BigEndian, &sysFlag)
		binary.Read(buf, binary.BigEndian, &bornTimeStamp)
		bornHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &bornHost)
		binary.Read(buf, binary.BigEndian, &bornPort)
		binary.Read(buf, binary.BigEndian, &storeTimestamp)
		storeHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &storeHost)
		binary.Read(buf, binary.BigEndian, &storePort)
		binary.Read(buf, binary.BigEndian, &reconsumeTimes)
		binary.Read(buf, binary.BigEndian, &preparedTransactionOffset)
		binary.Read(buf, binary.BigEndian, &bodyLength)
		if bodyLength > 0 {
			body = make([]byte, bodyLength)
			binary.Read(buf, binary.BigEndian, body)
		}
		binary.Read(buf, binary.BigEndian, &topicLen)
		topic = make([]byte, int(topicLen))
		binary.Read(buf, binary.BigEndian, topic)
		binary.Read(buf, binary.BigEndian, &propertiesLength)

		var propertiesMap map[string]string

		if propertiesLength > 0 {
			properties = make([]byte, propertiesLength)
			binary.Read(buf, binary.BigEndian, properties)
			propertiesMap = string2messageProperties(properties)
		}

		if magicCode != -626843481 {
			logger.Infof("magic code is error %d", magicCode)
			return nil
		}

		msg.Topic = string(topic)
		msg.QueueId = queueId
		msg.SysFlag = sysFlag
		msg.QueueOffset = queueOffset
		msg.BodyCRC = bodyCRC
		msg.StoreSize = storeSize
		msg.BornTimestamp = bornTimeStamp
		msg.ReconsumeTimes = reconsumeTimes
		msg.Flag = flag
		//msg.commitLogOffset=physicOffset
		msg.StoreTimestamp = storeTimestamp
		msg.PreparedTransactionOffset = preparedTransactionOffset
		msg.Body = body
		msg.Properties = propertiesMap

		msgs = append(msgs, msg)
	}

	return msgs
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
