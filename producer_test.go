package rocketmq

import (
	"testing"
)

func TestDefaultProducer_Send(t *testing.T) {
	SetLevel(testLogLevel)
	conf := &Config{
		Namesrv:       testNamesrvAddress,
		EnableUniqKey: true,
		Group:         testProducerGroup,
	}
	producer, _ := NewDefaultProducer(conf)
	producer.Start()
	tc := []struct {
		name    string
		runable func(producer Producer, t *testing.T)
	}{
		{
			"Send", func(producer Producer, t *testing.T) {
			msg := NewMessage(testProducerTopic, []byte("hello world"))
			res, err := producer.Send(msg)
			if err != nil {
				t.Log(err)
				t.Log(res)
				t.Fatal("send fail", err)
			}
		},
		},
		{
			"BatchSend", func(producer Producer, t *testing.T) {
			msgs := BatchMessage{}
			for i := 0; i < 100; i++ {
				msgs = append(msgs, NewMessage(testProducerTopic, []byte("hello world")))
			}
			res, err := producer.BatchSend(msgs)
			if err != nil {
				t.Log(err)
				t.Log(res)
				t.Fatal("send fail", err)
			}
		},
		},
		{"SendWithUniqKey", func(producer Producer, t *testing.T) {
			msg := NewMessage(testProducerTopic, []byte("hello world"))
			res, err := producer.Send(msg)
			t.Log(msg.GetProperty(UniqKey))
			if err != nil {
				t.Log(err)
				t.Log(res)
				t.Fatal("send fail", err)
			}
		}},
	}
	for _, c := range tc {
		t.Run(c.name, func(t *testing.T) {
			c.runable(producer, t)
		})
	}

}

// TODO move to mqclient

func TestDefaultProducer_SendMaxOpaqueAndNegtiveOpaque(t *testing.T) {
	//opaque = math.MaxInt32
	//conf := &Config{
	//	Namesrv:       "172.17.5.201:9876;172.17.5.203:9876",
	//	InstanceName:  "DEFAULT",
	//	EnableUniqKey: true,
	//	Group:         "itgroup",
	//}
	//producer, _ := NewDefaultProducer(conf)
	//producer.Start()
	//msg := NewMessage("itest", []byte("hello world"))
	//for i := 0; i < 10; i++ {
	//	res, err := producer.Send(msg)
	//	t.Log(res)
	//	t.Log(err)
	//	if err != nil {
	//		t.Log(err)
	//		t.Log(res)
	//		t.Fatal("send fail", err)
	//	}
	//}
}
