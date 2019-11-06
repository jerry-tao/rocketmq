package rocketmq

import (
	"errors"
	"strconv"
)

type Producer interface {
	Start()
	Shutdown()
	Send(msg *Message) (*SendResult, error)
	BatchSend(msgs BatchMessage) (*SendResult, error)
	SendAsync(msg *Message, sendCallback SendCallback) error
}

const (
	Sync = iota
	Async
)

type SendCallback func(result *SendResult, err error)

type DefaultProducer struct {
	conf     *Config
	mqClient mqClient
}

func NewDefaultProducer(conf *Config) (Producer, error) {
	err := validConfig(conf)
	if err != nil {
		return nil, err
	}
	producer := &DefaultProducer{
		conf:     conf,
		mqClient: newMqClient(conf),
	}
	return producer, nil
}

func (d *DefaultProducer) Start() {
	d.mqClient.start()
}

func (d *DefaultProducer) Shutdown() {
}

func (d *DefaultProducer) Send(msg *Message) (result *SendResult, err error) {
	if err = msg.checkMessage(d); err != nil {
		return
	}

	if d.conf.EnableUniqKey && msg.GetProperty(UniqKey) == "" {
		msg.SetProperty(UniqKey, genUniqKey())
	}
	return d.send(msg, Sync, nil)
}

func (d *DefaultProducer) BatchSend(msgs BatchMessage) (result *SendResult, err error) {
	if len(msgs) == 0 {
		return nil, errors.New("messages is empty")
	}

	for _, msg := range msgs {
		if d.conf.EnableUniqKey && msg.GetProperty(UniqKey) == "" {
			msg.SetProperty(UniqKey, genUniqKey())
		}
	}

	if msgs.Size() > d.conf.MaxMessageSize {
		return nil, errors.New("ResponseCode:" + strconv.Itoa(MsgIllegal) + ", the message body size over max value, MAX:" + strconv.Itoa(d.conf.MaxMessageSize))
	}

	return d.send(msgs, Sync, nil)
}

func (d *DefaultProducer) SendAsync(msg *Message, sendCallback SendCallback) (err error) {
	_, err = d.send(msg, Async, sendCallback)
	return
}

func (d *DefaultProducer) send(msg internalMessage, communicationMode int, sendCallback SendCallback) (sendResult *SendResult, err error) {

	return d.mqClient.sendMsg(msg, communicationMode, sendCallback) //fixme retrycount

}
