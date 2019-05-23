## Introduction

A RocketMQ client for golang supportting producer and consumer.

This is forked from https://github.com/sevennt/rocketmq. And fix some bugs.

- Producer concurrency bug.
- Producer uniq_key bug.
- Consumer lack of topic bug.
- Replace fmt with logger.
- Some other typo or minor fix.

## Import package

```
import "github.com/jerry-tao/rocketmq"
```

## Getting started

### Getting message with consumer

```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
var timeSleep = 30 * time.Second
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}

consumer, err := rocketmq.NewDefaultConsumer(consumerGroup, consumerConf)
if err != nil {
    return err
}
consumer.Subscribe(consumerTopic, "*")
consumer.RegisterMessageListener(
    func(msgs []*MessageExt) error {
        for i, msg := range msgs {
            logger.Info("msg", i, msg.Topic, msg.Flag, msg.Properties, string(msg.Body))
        }
        logger.Info("Consume success!")
        return nil
    })
consumer.Start()

time.Sleep(timeSleep)
```

### Sending message with producer

- Synchronous sending
```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}

producer, err := rocketmq.NewDefaultProducer(group, conf)
producer.Start()
if err != nil {
    return errors.New("NewDefaultProducer err")
}
msg := NewMessage(topic, []byte("Hello RocketMQ!")
if sendResult, err := producer.Send(msg); err != nil {
    return errors.New("Sync sending fail!")
} else {
    logger.Info("Sync sending success!, ", sendResult)
}
```

- Asynchronous sending

```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}
producer, err := rocketmq.NewDefaultProducer(group, conf)
producer.Start()
if err != nil {
    return err
}
msg := NewMessage(topic, []byte("Hello RocketMQ!")
sendCallback := func() error {
    logger.Info("I am callback")
    return nil
}
if err := producer.SendAsync(msg, sendCallback); err != nil {
    return err
} else {
    logger.Info("Async sending success!")
}
```

- Oneway sending

```
group := "dev-VodHotClacSrcData"
topic := "canal_vod_collect__video_collected_count_live"
conf := &rocketmq.Config{
    Nameserver:   "192.168.7.101:9876;192.168.7.102:9876;192.168.7.103:9876",
    ClientIp:     "192.168.1.23",
    InstanceName: "DEFAULT",
}
producer, err := rocketmq.NewDefaultProducer(group, conf)
producer.Start()
if err != nil {
    return err
}
msg := NewMessage(topic, []byte("Hello RocketMQ!")
if err := producer.SendOneway(msg); err != nil {
    return err
} else {
    logger.Info("Oneway sending success!")
}
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
msg.GetProperty(rocketmq.UNIQ_KEY)
```

### PullMsgNums 

```
// consumer config
conf := &rocketmq.Config{
    Namesrv:        "",
    InstanceName:   "DEFAULT",
    PullMaxMsgNums: -32,
}
```

**Notice: the auto generated uniqKey still have some bug, now you can consider it as random string.**

## Todo 

- Testing
- Data Race
- Improvement performance(consumer too slow)
    - ~~Add pull msg nums~~
- Split producer/Consumer config    