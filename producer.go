package rocketmq

type Producer interface {
	Start()
	Shutdown()
	Send(msg *Message) (*SendResult, error)
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

func (d *DefaultProducer) Send(msg *Message) (*SendResult, error) {
	return d.send(msg, Sync, nil)
}

func (d *DefaultProducer) SendAsync(msg *Message, sendCallback SendCallback) (err error) {
	_, err = d.send(msg, Async, sendCallback)
	return
}

func (d *DefaultProducer) send(msg *Message, communicationMode int, sendCallback SendCallback) (sendResult *SendResult, err error) {
	if err = msg.checkMessage(d); err != nil {
		return
	}

	if d.conf.EnableUniqKey && msg.GetProperty(UniqKey) == "" {
		msg.SetProperty(UniqKey, genUniqKey())
	}

	return d.mqClient.sendMsg(msg, communicationMode, sendCallback) //fixme retrycount

}
