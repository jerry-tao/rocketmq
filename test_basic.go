package rocketmq

var (
	testPrefix             = "rmqt"
	testNamesrvAddress     = "172.17.5.201:9876;172.17.5.203:9876"
	testBrokerAddress      = "127.0.0.1:10010"
	testProducerGroup      = testPrefix + "-rmqpg"
	testConsumerGroup      = testPrefix + "-rmqcg"
	testConsumerTopic      = testPrefix + "-rmqct" // need existed messages in this topic
	testProducerTopic      = testPrefix + "-rmqpt"
	testAutoCreateTopic    = false // need server support
	testAutoCreateSubGroup = false // need server support
)
