package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"sync/atomic"
)

const (
	RpcType   = 0
	rpcOneway = 1
)

var (
	opaque int32 = 1000
)

// response code
const (
	// 成功
	Success = 0
	// 发生了未捕获异常
	SysError = 1
	// 由于线程池拥堵，系统繁忙
	SysBusy = 2
	// 请求代码不支持
	RequestCodeNotSupported = 3
	//事务失败，添加db失败
	TransactionFailed = 4
	// Broker 刷盘超时
	FlushDiskTimeout = 10
	// Broker 同步双写，Slave不可用
	SlaveNotAvailable = 11
	// Broker 同步双写，等待Slave应答超时
	FlushSlaveTimeout = 12
	// Broker 消息非法
	MsgIllegal = 13
	// Broker, Namesrv 服务不可用，可能是正在关闭或者权限问题
	ServiceNotAvailable = 14
	// Broker, Namesrv 版本号不支持
	VersionNotSupported = 15
	// Broker, Namesrv 无权限执行此操作，可能是发、收、或者其他操作
	NoPermission = 16
	// Broker, Topic不存在
	TopicNotExist = 17
	// Broker, Topic已经存在，创建Topic
	TopicExistAlready = 18
	// Broker 拉消息未找到（请求的Offset等于最大Offset，最大Offset无对应消息）
	PullNotFound = 19
	// Broker 可能被过滤，或者误通知等
	PullRetryImmediately = 20
	// Broker 拉消息请求的Offset不合法，太小或太大
	PullOffsetMoved = 21
	// Broker 查询消息未找到
	QueryNotFound = 22
	// Broker 订阅关系解析失败
	SubscriptionParseFailed = 23
	// Broker 订阅关系不存在
	SubscriptionNotExist = 24
	// Broker 订阅关系不是最新的
	SubscriptionNotLatest = 25
	// Broker 订阅组不存在
	SubscriptionGroupNotExist = 26
	// Producer 事务应该被提交
	TransactionShouldCommit = 200
	// Producer 事务应该被回滚
	TransactionShouldRollback = 201
	// Producer 事务状态未知
	TransactionStateUnknow = 202
	// Producer ProducerGroup错误
	TransactionStateGroupWrong = 203
	// 单元化消息，需要设置 buyerId
	NoBuyerId = 204
	// 单元化消息，非本单元消息
	NotInCurrentUint = 205
	// Consumer不在线
	ConsumerNotOnline = 206
	// Consumer消费消息超时
	ConsumeMsgTimeout = 207
)

// request code
const (
	// Broker 发送消息
	SendMsg = 10
	// Broker 订阅消息
	PullMsg = 11
	// Broker 查询消息
	QueryMESSAGE = 12
	// Broker 查询Broker Offset
	// Broker 查询Consumer Offset
	QueryConsumerOffset = 14
	// Broker 更新Consumer Offset
	UpdateCconsumerOffset = 15
	// Broker 更新或者增加一个Topic
	UpdateAndCreateTopic = 17
	// Broker 获取所有Topic的配置（Slave和Namesrv都会向Master请求此配置）
	GetAllTopicConfig = 21
	// Broker 获取所有Topic配置（Slave和Namesrv都会向Master请求此配置）
	GetTopicConfigList = 22
	// Broker 获取所有Topic名称列表
	GetTopicNameList = 23
	// Broker 更新Broker上的配置
	UpdateBrokerConfig = 25
	// Broker 获取Broker上的配置
	GetBrokerConfig = 26
	// Broker 触发Broker删除文件
	TriggerDeleteFILES = 27
	// Broker 获取Broker运行时信息
	GetBrokerRuntimeInfo = 28
	// Broker 根据时间查询队列的Offset
	SearchOffsetByTimeStamp = 29
	// Broker 查询队列最大Offset
	GetMaxOffset = 30
	// Broker 查询队列最小Offset
	GetMinOffset = 31
	// Broker 查询队列最早消息对应时间
	GetEarliestMsgStoreTime = 32
	// Broker 根据消息ID来查询消息
	ViewMsgById = 33
	// Broker Client向Client发送心跳，并注册自身
	HeartBeat = 34
	// Broker Client注销
	UnregisterClient = 35
	// Broker Consumer将处理不了的消息发回服务器
	CconsumerSendMsgBack = 36
	// Broker Commit或者Rollback事务
	EndTransaction = 37
	// Broker 获取ConsumerId列表通过GroupName
	GetConsumerListByGroup = 38
	// Broker 主动向Producer回查事务状态
	CheckTransactionState = 39
	// Broker Broker通知Consumer列表变化
	NotifyConsumerIdsChanged = 40
	// Broker Consumer向Master锁定队列
	LockBatchMq = 41
	// Broker Consumer向Master解锁队列
	UNLockBatchMq = 42
	// Broker 获取所有Consumer Offset
	GetAllCconsumerOffset = 43
	// Broker 获取所有定时进度
	GetAllDelayOffset = 45
	// Namesrv 向Namesrv追加KV配置
	PutKVConfig = 100
	// Namesrv 从Namesrv获取KV配置
	GetKVConfig = 101
	// Namesrv 从Namesrv获取KV配置
	DeleteKVConfig = 102
	// Namesrv 注册一个Broker，数据都是持久化的，如果存在则覆盖配置
	RegisterBroker = 103
	// Namesrv 卸载一个Broker，数据都是持久化的
	UnregisterBroker = 104
	// Namesrv 根据Topic获取Broker Name、队列数(包含读队列与写队列)
	GetRouteinfoByTopic = 105
	// Namesrv 获取注册到Name Server的所有Broker集群信息
	GetBrokerClusterInfo             = 106
	UpdateAndCreateSubscriptionGroup = 200
	GetAllSubscriptionGroupConfig    = 201
	GetTopicStatsInfo                = 202
	GetConsumerConnList              = 203
	GetProducerConnList              = 204
	WipeWritePermOfBroker            = 205

	// 从Name Server获取完整Topic列表
	GetAllTopicListFromNamesrv = 206
	// 从Broker删除订阅组
	DeleteSubscriptionGroup = 207
	// 从Broker获取消费状态（进度）
	GetConsumeStats = 208
	// Suspend Consumer消费过程
	SuspendConsumer = 209
	// Resume Consumer消费过程
	ResumeConsumer = 210
	// 重置Consumer Offset
	ResetCconsumerOffsetInConsumer = 211
	// 重置Consumer Offset
	ResetCconsumerOffsetInBroker = 212
	// 调整Consumer线程池数量
	AdjustCconsumerThreadPoolPOOL = 213
	// 查询消息被哪些消费组消费
	WhoConsumeTHE_MESSAGE = 214

	// 从Broker删除Topic配置
	DeleteTopicInBroker = 215
	// 从Namesrv删除Topic配置
	DeleteTopicInNamesrv = 216
	// Namesrv 通过 project 获取所有的 server ip 信息
	GetKvConfigByValue = 217
	// Namesrv 删除指定 project group 下的所有 server ip 信息
	DeleteKvConfigByValue = 218
	// 通过NameSpace获取所有的KV List
	GetKvlistByNamespace = 219

	// offset 重置
	ResetCconsumerClientOffset = 220
	// 客户端订阅消息
	GetCconsumerStatusFromClient = 221
	// 通知 broker 调用 offset 重置处理
	InvokeBrokerToResetOffset = 222
	// 通知 broker 调用客户端订阅消息处理
	InvokeBrokerToGetCconsumerSTATUS = 223

	// Broker 查询topic被谁消费
	// 2014-03-21 Add By shijia
	QueryTopicConsumeByWho = 300

	// 获取指定集群下的所有 topic
	// 2014-03-26
	GetTopicsByCluster = 224

	// 向Broker注册Filter Server
	// 2014-04-06 Add By shijia
	RegisterFilterServer = 301
	// 向Filter Server注册Class
	// 2014-04-06 Add By shijia
	RegisterMsgFilterClass = 302
	// 根据 topic 和 group 获取消息的时间跨度
	QueryConsumeTimeSpan = 303
	// 获取所有系统内置 Topic 列表
	GetSysTopicListFromNS     = 304
	GetSysTopicListFromBroker = 305

	// 清理失效队列
	CleanExpiredConsumequeue = 306

	// 通过Broker查询Consumer内存数据
	// 2014-07-19 Add By shijia
	GetCconsumerRunningInfo = 307

	// 查找被修正 offset (转发组件）
	QueryCorrectionOffset = 308

	// 通过Broker直接向某个Consumer发送一条消息，并立刻消费，返回结果给broker，再返回给调用方
	// 2014-08-11 Add By shijia
	ConsumeMsgDirectly = 309

	// Broker 发送消息，优化网络数据包
	SendMsgV2 = 310

	// 单元化相关 topic
	GetUnitTopicList = 311
	// 获取含有单元化订阅组的 Topic 列表
	GetHasUnitSubTopicList = 312
	// 获取含有单元化订阅组的非单元化 Topic 列表
	GetHasUnitSubUnunitTopicList = 313
	// 克隆某一个组的消费进度到新的组
	CloneGroupOffset = 314

	// 查看Broker上的各种统计信息
	ViewBrokerStatsData = 315
)

type RemotingCommand struct {
	// header
	Code      int         `json:"code"`
	Language  string      `json:"language"`
	Version   int         `json:"version"`
	Opaque    int32       `json:"opaque"`
	Flag      int         `json:"flag"`
	Remark    string      `json:"remark"`
	ExtFields interface{} `json:"extFields"`
	// body
	Body []byte `json:"-"`
}

func buildCommand(code int, ext interface{}, body []byte) *RemotingCommand {
	return &RemotingCommand{
		Code:      code,
		ExtFields: ext,
		Body:      body,
		Language:  language,
		Opaque:    genOpaque(),
		Version:   version,
	}
}

func (r *RemotingCommand) String() string {
	j, _ := json.Marshal(r)
	return string(j)
}

func (r *RemotingCommand) buildHeader() []byte {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	return buf
}

func (r *RemotingCommand) encode() ([]byte, error) {
	header := r.buildHeader()
	body := r.Body
	buf := bytes.NewBuffer([]byte{})
	err := binary.Write(buf, binary.BigEndian, int32(len(header)+len(body)+4))
	err = binary.Write(buf, binary.BigEndian, int32(len(header)))
	err = binary.Write(buf, binary.BigEndian, header)
	if len(body) > 0 {
		err = binary.Write(buf, binary.BigEndian, body)
	}
	return buf.Bytes(), err
}

func (r *RemotingCommand) decodeCommandCustomHeader() (responseHeader SendMessageResponseHeader) {
	msgID := r.ExtFields.(map[string]interface{})["msgId"].(string)
	queueID, _ := strconv.Atoi(r.ExtFields.(map[string]interface{})["queueId"].(string))
	queueOffset, _ := strconv.Atoi(r.ExtFields.(map[string]interface{})["queueOffset"].(string))
	responseHeader = SendMessageResponseHeader{
		msgId:         msgID,
		queueId:       int32(queueID),
		queueOffset:   int64(queueOffset),
		transactionId: "",
	}
	return
}

func (r *RemotingCommand) markOneWayRPC() {
	bits := 1 << rpcOneway
	r.Flag |= bits
}

func (r *RemotingCommand) equal(o *RemotingCommand) bool {
	if len(r.Body) != len(o.Body) {
		return false
	}
	for i, b := range r.Body {
		if b != o.Body[i] {
			return false
		}
	}
	return true
}

// ext fields

type ConsumerData struct {
	GroupName           string
	ConsumerType        string
	MessageModel        string
	ConsumeFromWhere    string
	SubscriptionDataSet []*SubscriptionData
	UnitMode            bool
}

type HeartbeatData struct {
	ClientId        string
	ConsumerDataSet []*ConsumerData
}

type LockBatchRequest struct {
	ConsumerGroup string          `json:"consumerGroup"`
	ClientId      string          `json:"clientId"`
	MqSet         []*messageQueue `json:"mqSet"`
}

type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
}

type GetRouteInfoRequestHeader struct {
	topic string
}

func (g *GetRouteInfoRequestHeader) MarshalJSON() ([]byte, error) {
	//return []byte(`{"topic":"`+g.topic+`"}`),nil
	var buf bytes.Buffer
	buf.WriteString("{\"topic\":\"")
	buf.WriteString(g.topic)
	buf.WriteString("\"}")
	return buf.Bytes(), nil
}

type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

type GetConsumerListByGroupResponseBody struct {
	ConsumerIDList []string
}

func genOpaque() int32 {
	currOpaque := atomic.AddInt32(&opaque, 1)
	if currOpaque >= 0 && currOpaque <= 1000 {
		currOpaque = atomic.AddInt32(&opaque, 1001)
	}
	return currOpaque
}

func (r *RemotingCommand) decodeMessage() []*MessageExt {
	data := r.Body
	buf := bytes.NewBuffer(data)
	var storeSize, magicCode, bodyCRC, queueId, flag, sysFlag, reconsumeTimes, bodyLength, bornPort, storePort int32
	var queueOffset, physicOffset, preparedTransactionOffset, bornTimeStamp, storeTimestamp int64
	var topicLen byte
	var topic, body, properties, bornHost, storeHost []byte
	var propertiesLength int16

	// var propertiesMap map[string]string

	msgs := make([]*MessageExt, 0, 32)
	for buf.Len() > 0 {
		msg := new(MessageExt)
		binary.Read(buf, binary.BigEndian, &storeSize)
		binary.Read(buf, binary.BigEndian, &magicCode)
		binary.Read(buf, binary.BigEndian, &bodyCRC)
		binary.Read(buf, binary.BigEndian, &queueId)
		binary.Read(buf, binary.BigEndian, &flag)
		binary.Read(buf, binary.BigEndian, &queueOffset)
		binary.Read(buf, binary.BigEndian, &physicOffset)
		binary.Read(buf, binary.BigEndian, &sysFlag)
		binary.Read(buf, binary.BigEndian, &bornTimeStamp)
		bornHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &bornHost)
		binary.Read(buf, binary.BigEndian, &bornPort)
		binary.Read(buf, binary.BigEndian, &storeTimestamp)
		storeHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &storeHost)
		binary.Read(buf, binary.BigEndian, &storePort)
		binary.Read(buf, binary.BigEndian, &reconsumeTimes)
		binary.Read(buf, binary.BigEndian, &preparedTransactionOffset)
		binary.Read(buf, binary.BigEndian, &bodyLength)
		if bodyLength > 0 {
			body = make([]byte, bodyLength)
			binary.Read(buf, binary.BigEndian, body)
		}
		binary.Read(buf, binary.BigEndian, &topicLen)
		topic = make([]byte, int(topicLen))
		binary.Read(buf, binary.BigEndian, topic)
		binary.Read(buf, binary.BigEndian, &propertiesLength)

		var propertiesMap map[string]string

		if propertiesLength > 0 {
			properties = make([]byte, propertiesLength)
			binary.Read(buf, binary.BigEndian, properties)
			propertiesMap = string2messageProperties(properties)
		}

		if magicCode != -626843481 {
			logger.Debug(r)
			logger.Errorf("magic code is error %d", magicCode)
			return nil
		}

		msg.Topic = string(topic)
		msg.QueueId = queueId
		msg.SysFlag = sysFlag
		msg.QueueOffset = queueOffset
		msg.BodyCRC = bodyCRC
		msg.StoreSize = storeSize
		msg.BornTimestamp = bornTimeStamp
		msg.ReconsumeTimes = reconsumeTimes
		msg.Flag = flag
		//msg.commitLogOffset=physicOffset
		msg.StoreTimestamp = storeTimestamp
		msg.PreparedTransactionOffset = preparedTransactionOffset
		msg.Body = body
		msg.Properties = propertiesMap

		msgs = append(msgs, msg)
	}

	return msgs
}
