package rocketmq

// Config struct
type Config struct {
	// general config
	Namesrv       string
	ClientIP      string
	Group         string
	InstanceName  string
	TimeoutMillis int64
	// consumer config
	PullMaxMsgNums int32 // MIN 1 小于1的会当做1 MAX 32 超过32的数字会被当做32对待
	// producer config
	EnableUniqKey                    bool
	RetryTimes                       int
	DefaultTopicQueueNums            int
	RetryAnotherBrokerWhenNotStoreOK bool
	MaxMessageSize                   int
}

func validConfig(config *Config) error {
	if config == nil {
		return ErrConfigNil
	}
	if config.Namesrv == "" {
		return ErrNameSrv
	}

	if config.Group == "" || config.Group == defaultConsumerGroup || config.Group == defaultProducerGroup {
		return ErrGroup
	}

	if config.PullMaxMsgNums <= 0 || config.PullMaxMsgNums > 32 {
		config.PullMaxMsgNums = defaultPullMaxMsgsNums
		logger.Debugf("Invalid PullMaxMsgNums %d, use default 32", config.PullMaxMsgNums)
	}

	if config.ClientIP == "" {
		config.ClientIP = localIP.String()
		logger.Debug("Undefined ClientIP use default local ip: ", config.ClientIP)
	}

	if config.TimeoutMillis < 1000 {
		config.TimeoutMillis = defaultGlobalTimeout
		logger.Debug("Timeout too small, use default timeout 5000ms")
	}

	if config.DefaultTopicQueueNums < 4 {
		config.DefaultTopicQueueNums = defaultTopicQueueNums
		logger.Debug("Default topic queue nums too small, change to default 4")
	}

	if config.RetryTimes < 1 {
		config.RetryTimes = defaultRetryTimes
		logger.Debug("Retry times invalid, set to default 2")
	}

	if config.MaxMessageSize < 1024{
		config.MaxMessageSize = defaultMaxMessageSize
		logger.Debug("MaxMessageSize invalid, set to default 4096")
	}

	return nil
}

//type ConsumerConfig struct {
//}
//type ProducerConfig struct {
//}
