## Introduction

A RocketMQ client for golang supporting producer and consumer.

This is forked from https://github.com/sevennt/rocketmq.

Now this project is still in testing.


## Feature unsupport

### Producer Oneway

During test, i found it highly unreliable, when keep pushing msg to server, if server can't handle it, it will simply drop the msg, the drop ratio is very high around 10~30%, so unless you really don't care send it or not, otherwise you should never use oneway.

### Flag compress

The compress is on client, you can simple compress it by your self. If you really need the feature, you can wrap producer and consumer, more easy way.

### Msg Filter @TBD

### Batch send

No plan.

## Import package

```
import "github.com/jerry-tao/rocketmq"
```

## Getting started

### Getting message with consumer

```
conf := &rocketmq.Config{
		Namesrv:        "192.168.0.101:9876;192.168.0.102:9876",
		InstanceName:   strconv.Itoa(i),
		PullMaxMsgNums: 32,
		Group:          "test_topic",
	}

consumer, _ := rocketmq.NewDefaultConsumer(conf)
consumer.Subscribe("test_topic", "*")
consumer.RegisterMessageListener(
    func(msgs []*rocketmq.MessageExt) error {
        a := atomic.AddInt64(&amount, int64(len(msgs)))
        if a%100000 == 0 {
            log.Println("run time: ", time.Since(now), "produce: ", a)
        }
        return nil
    })
consumer.Start()
```

### Sending message with producer

```
conf := &rocketmq.Config{
    Namesrv:               "192.168.0.101:9876;192.168.0.102:9876",
    EnableUniqKey:         true,
    Group:                 "final",
    DefaultTopicQueueNums: 16,
}
producer, _ := rocketmq.NewDefaultProducer(conf)
producer.Start()
msg := rocketmq.NewMessage(topic, []byte("2myq"+strconv.Itoa(int(value))))
r, e := cli.Send(msg)
```


## Other

### Logger

Default Print nothing, and ship with a default logger.

```
rocketmq.SetOutput(writer)
rocketmq.SetLevel(rocketmq.InfoLevel)
// 	EmptyLevel = iota
// 	FatalLevel
// 	ErrorLevel
//  InfoLevel
// 	DebugLevel
```

Or set a logger imp the rocketmq.Logger interface:

```
//type Logger interface {
//	Debug(v ...interface{})
//	Debugf(format string, v ...interface{})
//	Info(v ...interface{})
//	Infof(format string, v ...interface{})
//	Error(v ...interface{})
//	Errorf(format string, v ...interface{})
//	Fatal(v ...interface{})
//	Fatalf(format string, v ...interface{})
//}

rocketmq.SetLogger(myLogger)
```

### UNIQ_KEY

The auto generate UNIQ_KEY is off by default, you could set by your self based on requirements:


```
msg := NewMessage(topic, []byte("Hello RocketMQ!")
msg.SetProperty(rocketmq.UniqKey, yourOwnUniqKey)
```

Or enable auto generate by config:

```
conf := &rocketmq.Config{
    Namesrv:       "",
    InstanceName:  "DEFAULT",
    EnableUniqKey: true,
}
```

Then you can get it later:

```
msg := NewMessage(topic, []byte("Hello RocketMQ!")
// after send
msg.GetProperty(rocketmq.UNIQ_KEY)
```

**Notice: the auto generated uniqKey still have some bug, now you can consider it as random string.**

### PullMsgNums 

```
// consumer config
conf := &rocketmq.Config{
    Namesrv:        "",
    PullMaxMsgNums: 32,
}
```

**The range for pullMaxMsgNums is 1~32**


### Graceful shutdown consumer

```
consumer.Shutdown()
```

**Don't shutdown consumer in message callback, consumer shutdown will wait all callback finish, since callback is waiting consumer shutdown, it will cause a loop dead block.**

TODO: now shutdown will wait until server close the connection, so it could be long.

## Todo 

- Testing
- Data Race
- ~~Improvement performance(consumer too slow)~~
    - ~~Add pull msg nums~~
- ~~Consumer graceful shutdown~~
- ~~Consumer reblance error~~
- Response timeout fix
- Vip channel
