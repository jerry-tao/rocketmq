package rocketmq

const (
	defaultTopic           = "TBW102"
	defaultProducerGroup   = "DEFAULT_PRODUCER"
	defaultConsumerGroup   = "DEFAULT_CONSUMER"
	defaultPullMaxMsgsNums = 32
	language               = "GOLANG"
	version                = 79
	masterID               = 0
	retryGroupTopicPrefix  = "%RETRY%"
	defaultTopicQueueNums  = 4
	defaultGlobalTimeout   = int64(5000)
	defaultRetryTimes      = 2
	defaultMaxMessageSize  = 1024 * 4
)
