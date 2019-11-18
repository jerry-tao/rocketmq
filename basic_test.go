package rocketmq

var (
	testPrefix             = "rmqt"
	testNamesrvAddress     = "172.17.5.201:9876;172.17.5.203:9876"
	testBrokerAddress      = "172.17.5.201:10010"
	testProducerGroup      = testPrefix + "-rmqpg"
	testConsumerGroup      = testPrefix + "-rmqcg"
	testConsumerTopic      = testPrefix + "-rmqct" // need existed messages in this topic
	testConsumerNotExist   = testPrefix + "-whoami"
	testProducerTopic      = testPrefix + "-rmqpt"
	testAutoCreateTopic    = false // need server support
	testAutoCreateSubGroup = false // need server support
	testLogLevel           = DebugLevel
)
