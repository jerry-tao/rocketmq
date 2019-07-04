package rocketmq

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestNewMqClient(t *testing.T) {
	c := newMqClient(&Config{
		Namesrv:        "172.17.5.201:9876;172.17.5.203:9876",
		PullMaxMsgNums: 32,
		Group:          "2bconsumer690",
	})
	if c == nil {
		t.Fatal("client should not be nil")
	}
}
func TestDefaultMqClient_sendMsg(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
	}{
		{
			name: "send sync with exist mq",
			msg:  NewMessage("exist_mq", []byte("test")),
		},
		{
			name: "send sync with auto create mq",
			msg:  NewMessage("autocreate_mq"+strconv.Itoa(rand.Int()), []byte("test")),
		},
	}
	var client = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}

	client.start()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := client.sendMsg(test.msg, Sync, nil)
			t.Log(res, err)
			if err == nil && res == nil {
				t.Fatal("send msg fail with nil nil")
			}
			if err != nil || res.SendStatus != 0 {
				t.Fatal("send msg fail")
			}
		})
	}
}
func TestDefaultMqClient_getTopicKernel(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{
			name:  "get exist mq from namesrv",
			topic: "exist_mq",
		},
		{
			name:  "get auto create mq from namesrv",
			topic: defaultTopic,
		},
		{
			name:  "get non exist mq from namesrv",
			topic: "not_exist_mq",
		},
	}
	var client = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}

	client.start()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := client.getTopicKernel(test.topic, 10000)
			if err == nil && res == nil {
				t.Fatal("get topic fail with nil nil")
			}
			switch test.name {
			case "get non exist mq from namesrv":
				if err != ErrTopicNotExist {
					t.Fatal("non exist mq fail")
				}
			default:
				if err != nil || res == nil || res.empty() {
					t.Fatal("get topic  fail", err)
				}
			}

		})
	}
}
func TestDefaultMqClient_tryGetTopic(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{
			name:  "get exist mq",
			topic: "exist_mq",
		},
		{
			name:  "get auto_create exist",
			topic: "autocreate_mq" + strconv.Itoa(rand.Int()),
		},
	}
	var client = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}

	client.start()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := client.getTopic(test.topic, true)
			if err == nil && res == nil {
				t.Fatal("get topic fail with nil nil")
			}
			switch test.name {
			default:
				if err != nil || res == nil {
					t.Log(res)
					t.Fatal("get topic  fail", err)
				}
			}

		})
	}
}

func TestDefaultMqClient_lockMq(t *testing.T) {
	var client = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}

	client.start()

	var c2 = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}
	c2.start()
	res, err := client.getTopic("exist_mq", false)
	c2.getTopic("exist_mq", false)
	t.Log(res)
	if err != nil {
		t.Error(err)
	}
	err = client.lockMq("tgo1", res.mqs[2])
	if err != nil {
		t.Error(err)
	}
	err = c2.lockMq("tgo1", res.mqs[2])
	t.Log(err)
	if err == nil {
		t.Error("err should not be nil")
	}
	client.unlockMq("tgo1", res.mqs[2])

}

func TestDefaultMqClient_unlockMq(t *testing.T) {
	var client = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}

	client.start()

	var c2 = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}
	c2.start()
	c2.getTopic("exist_mq", false)
	res, err := client.getTopic("exist_mq", false)
	t.Log(res)
	if err != nil {
		t.Error(err)
	}
	err = client.lockMq("tgo1", res.mqs[3])
	if err != nil {
		t.Error(err)
	}
	err = client.unlockMq("tgo1", res.mqs[3])
	if err != nil {
		t.Error(err)
	}

	err = c2.lockMq("tgo1", res.mqs[3])
	if err != nil {
		t.Log(err)
		t.Error("err should be nil")
	}

	err = c2.unlockMq("tgo1", res.mqs[3])
	if err != nil {
		t.Error("err should be nil")
	}

}

func TestDefaultMqClient_getOffset(t *testing.T) {
	var client = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}

	client.start()

	res, _ := client.getTopic("2myq", false)
	offset, err := client.getOffset("2bconsumer690", res.mqs[0])
	if err != nil {
		t.Fatal("get offset fail")
	}
	err = client.updateOffset("2bconsumer690", res.mqs[0], offset+32)
	if err != nil {
		t.Fatal("update offset fail")
	}
	newOffset, err := client.getOffset("2bconsumer690", res.mqs[0])
	if err != nil || newOffset-offset != 32 {
		t.Fatal("update offset result uncorrect")
	}

}

func TestDefaultMqClient_findConsumerIdList(t *testing.T) {
	//SetLevel(DebugLevel)
	conf := &Config{
		Namesrv:        "172.17.5.201:9876;172.17.5.203:9876",
		PullMaxMsgNums: 32,
		Group:          "2bconsumer690",
	}
	consumer, _ := NewDefaultConsumer(conf)
	consumer.Subscribe("2myq", "*")
	consumer.RegisterMessageListener(
		func(msgs []*MessageExt) error {
			t.Log(msgs)
			return nil
		})
	consumer.Start()

	c2, _ := NewDefaultConsumer(conf)
	c2.Subscribe("2myq", "*")
	c2.RegisterMessageListener(
		func(msgs []*MessageExt) error {
			t.Log(msgs)
			return nil
		})
	c2.Start()

	var c1 = &defaultMqClient{
		namesrv:         "172.17.5.201:9876;172.17.5.203:9876",
		brokerAddrTable: make(map[string]map[string]string),
		topics:          map[string]*topicInfo{},
		clientId:        localIP.String() + "@" + strconv.Itoa(os.Getpid()) + "@" + strconv.FormatInt(rand.Int63(), 10),
		ch:              make(chan struct{}),
		remotingClient:  NewDefaultRemotingClient(),
	}
	c1.start()
	res, err := c1.findConsumerIdList("2myq", "2bconsumer690")
	t.Log(res)
	t.Log(err)
	if err != nil || len(res) == 0 {
		t.Fatal("find consumer list fail")
	}
}
