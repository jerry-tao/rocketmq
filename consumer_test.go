package rocketmq

import (
	"github.com/pborman/uuid"
	"testing"
	"time"
)

func TestNewDefaultConsumer(t *testing.T) {
	conf := &Config{
		Namesrv:        "172.17.5.201:9876;172.17.5.203:9876",
		InstanceName:   uuid.New(),
		PullMaxMsgNums: 32,
		Group:          uuid.New(),
	}
	consumer, _ := NewDefaultConsumer(conf)
	consumer.Subscribe("test_topic", "*")
	consumer.RegisterMessageListener(
		func(msgs []*MessageExt) error {
			t.Log(msgs)
			return nil
		})
	consumer.Start()
	time.Sleep(time.Second*5)
	consumer.Shutdown()
}

func TestNewConsumerConsumeUniqueKey(t *testing.T) {
	//group := "2bconsumer597"
	//conf := &Config{
	//	Namesrv:        "172.17.5.201:9876;172.17.5.203:9876",
	//	InstanceName:   uuid.New(),
	//	PullMaxMsgNums: 32,
	//}
	//pullMessageService := NewPullMessageService()
	//mqClient := newMqClient(conf.ClientIP, conf.Namesrv, conf.InstanceName)
	//rebalance := NewRebalance(group, mqClient)
	//
	//consumer := &DefaultConsumer{
	//	conf:               conf,
	//	pullMessageService: pullMessageService,
	//	consumerGroup:      group,
	//	consumeFromWhere:   "CONSUME_FROM_LAST_OFFSET",
	//	messageModel:       "CLUSTERING",
	//	offsetStore:        newOffsetStore(mqClient, group),
	//	rebalance:          rebalance,
	//	mqClient:           mqClient,
	//	closeCh:            make(chan struct{}),
	//}
	//
	//ch := make(chan struct{})
	//rebalance.consumer = consumer
	//pullMessageService.service = consumer
	//consumer.Subscribe("2myq", "*")
	//consumer.RegisterMessageListener(
	//	func(msgs []*MessageExt) error {
	//		defer func() { recover(); os.Exit(0) }()
	//		t.Log(msgs)
	//		t.Log(msgs[0].GetProperty(UniqKey))
	//		close(ch)
	//		return nil
	//	})
	//consumer.Start()
	//consumer.Shutdown()
	//<-ch
}
