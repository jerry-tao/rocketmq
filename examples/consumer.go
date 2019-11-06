package main

import (
	"fmt"
	. "github.com/jerry-tao/rocketmq"
)

func main() {
	SetLevel(DebugLevel)
	conf := &Config{
		Namesrv:        "172.17.5.201:9876;172.17.5.203:9876",
		EnableUniqKey:  true,
		PullMaxMsgNums: 1,
		Group:          "rrrr",
	}
	consumer,_  :=NewDefaultConsumer(conf)
	consumer.Subscribe("rrr-rrr","*")
	consumer.RegisterMessageListener(func(msgs []*MessageExt) error {
		fmt.Println(len(msgs))
		return nil
	})
	consumer.Start()
	select {

	}

}
