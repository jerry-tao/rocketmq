package rocketmq

import (
	"errors"
	"testing"
)

func TestNewDefaultConsumer(t *testing.T) {
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
				go func() {
					consumer.Shutdown()
					ch <- struct{}{}
				}()
			}
			// return error won't update offset
			return errors.New("empty")
		})
	_ = consumer.Start()
	<-ch
}
