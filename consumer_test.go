package rocketmq

import (
	"testing"
)

func TestNewDefaultConsumer(t *testing.T) {
	SetLevel(DebugLevel)
	ch := make(chan struct{})
	conf := &Config{
		Namesrv:        testNamesrvAddress,
		PullMaxMsgNums: 32,
		Group:          testConsumerGroup,
	}
	consumer, _ := NewDefaultConsumer(conf)
	consumer.Subscribe(testConsumerTopic, "*")
	consumer.RegisterMessageListener(
		func(msgs []*MessageExt) error {
			if len(msgs) == 0 {
				t.Fatalf("consumer empty msgs")
			} else {
				consumer.ResetOffset(testConsumerTopic)
				close(ch)
				go consumer.Shutdown()
			}
			return nil
		})
	_ = consumer.Start()
	<-ch
}
