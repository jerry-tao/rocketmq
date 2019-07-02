package rocketmq

import "strconv"

const (
	defaultSuspend = 1000      // 1s
	maxSuspend     = 16 * 1000 // 16s
)

type PullRequest struct {
	consumerGroup string
	messageQueue  *messageQueue
	nextOffset    int64
	suspend       int64
}

func (pr *PullRequest) String() string {
	return pr.messageQueue.String() + ":" + strconv.FormatInt(pr.nextOffset, 10)
}

type PullMessageRequestHeader struct {
	ConsumerGroup        string `json:"consumerGroup"`
	Topic                string `json:"topic"`
	QueueId              int32  `json:"queueId"`
	QueueOffset          int64  `json:"queueOffset"`
	MaxMsgNums           int32  `json:"maxMsgNums"`
	SysFlag              int32  `json:"sysFlag"`
	CommitOffset         int64  `json:"commitOffset"`
	SuspendTimeoutMillis int64  `json:"suspendTimeoutMillis"`
	Subscription         string `json:"subscription"`
	SubVersion           int64  `json:"subVersion"`
}

type Service interface {
	pullMessage(pullRequest *PullRequest)
}

type PullMessageService struct {
	pullRequestQueue chan *PullRequest
	service          Service
}

func NewPullMessageService() *PullMessageService {
	return &PullMessageService{
		pullRequestQueue: make(chan *PullRequest, 1024),
	}
}

func (p *PullMessageService) start() {
	for {
		pullRequest := <-p.pullRequestQueue
		p.service.pullMessage(pullRequest)
	}
}
