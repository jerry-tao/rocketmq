package rocketmq

import (
	"math"
	"testing"
)

func TestDefaultProducer_Send(t *testing.T) {
	SetLevel(DebugLevel)
	conf := &Config{
		Namesrv:       "172.17.5.201:9876;172.17.5.203:9876",
		InstanceName:  "DEFAULT",
		EnableUniqKey: true,
		Group:         "itgroup",
	}
	producer, _ := NewDefaultProducer(conf)
	producer.Start()
	msg := NewMessage("fjlasjdf", []byte("hello world"))
	msg.SetProperty(UniqKey, "0ae026b5b7be15111a6cd07b1bc300854cb9f7068c0489f")
	res, err := producer.Send(msg)
	t.Log(res)
	t.Log(err)
	if err != nil {
		t.Log(err)
		t.Log(res)
		t.Fatal("send fail", err)
	}
}
func TestDefaultProducer_SendUniqKey(t *testing.T) {
	conf := &Config{
		Namesrv:       "172.17.5.201:9876;172.17.5.203:9876",
		InstanceName:  "DEFAULT",
		EnableUniqKey: true,
		Group:         "itgroup",
	}
	producer, _ := NewDefaultProducer(conf)
	producer.Start()
	msg := NewMessage("itest1", []byte("hello world"))
	res, err := producer.Send(msg)
	t.Log(msg.GetProperty(UniqKey))
	t.Log(res)
	t.Log(err)
	if err != nil {
		t.Log(err)
		t.Log(res)
		t.Fatal("send fail", err)
	}
}

func TestDefaultProducer_SendMaxOpaqueAndNegtiveOpaque(t *testing.T) {
	opaque = math.MaxInt32
	conf := &Config{
		Namesrv:       "172.17.5.201:9876;172.17.5.203:9876",
		InstanceName:  "DEFAULT",
		EnableUniqKey: true,
		Group:         "itgroup",
	}
	producer, _ := NewDefaultProducer(conf)
	producer.Start()
	msg := NewMessage("itest", []byte("hello world"))
	for i := 0; i < 10; i++ {
		res, err := producer.Send(msg)
		t.Log(res)
		t.Log(err)
		if err != nil {
			t.Log(err)
			t.Log(res)
			t.Fatal("send fail", err)
		}
	}
}
