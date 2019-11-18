package rocketmq

import (
	"errors"
	"testing"
	"time"
)

func TestNewDefaultConsumer(t *testing.T) {
	SetLevel(testLogLevel)
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
	select {
	case <-ch:
	case <-time.After(time.Second * 30):
		consumer.Shutdown()
		t.Fatal("over 30 second, is the test topic has data for consume, check debug log for detail")
	}
}


func TestConsumeNotExistTopic(t *testing.T) {
	SetLevel(testLogLevel)
	ch := make(chan struct{})
	conf := &Config{
		Namesrv:        testNamesrvAddress,
		PullMaxMsgNums: 32,
		Group:          testConsumerGroup,
	}
	consumer, _ := NewDefaultConsumer(conf)
	consumer.Subscribe(testConsumerNotExist, "*")
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
	select {
	case <-ch:
	case <-time.After(time.Second * 120):
		consumer.Shutdown()
		t.Fatal("over 30 second, is the test topic has data for consume, check debug log for detail")
	}
}

