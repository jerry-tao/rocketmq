package rocketmq

import "testing"

func Test_allocate(t *testing.T) {
	amq := new(AllocateMessageQueueAveragely)
	mqs := messageQueues{
		{topic: "2myq",
			brokerName: "rocketmq1",
			queueId:    0,
		},
		{topic: "2myq",
			brokerName: "rocketmq1",
			queueId:    1,
		},
		{topic: "2myq",
			brokerName: "rocketmq1",
			queueId:    2,
		},
		{topic: "2myq",
			brokerName: "rocketmq1",
			queueId:    3,
		},
		{topic: "2myq",
			brokerName: "rocketmq2",
			queueId:    0,
		},
		{topic: "2myq",
			brokerName: "rocketmq2",
			queueId:    1,
		},
		{topic: "2myq",
			brokerName: "rocketmq2",
			queueId:    2,
		}, {topic: "2myq",
			brokerName: "rocketmq2",
			queueId:    3,
		},
	}
	res, err := amq.allocate("demo", "2@10.224.38.181@40832", mqs, []string{"0@10.224.38.181@40832",
		"1@10.224.38.181@40832", "2@10.224.38.181@40832", "3@10.224.38.181@40832", "4@10.224.38.181@40832",
		"5@10.224.38.181@40832", "6@10.224.38.181@40832", "7@10.224.38.181@40832"})
	t.Log(res, err)
	res2, err := amq.allocate("demo", "1", mqs, []string{"0", "1", "2"})
	t.Log(res2, err)
	res3, err := amq.allocate("demo", "2", mqs, []string{"0", "1", "2"})
	t.Log(res3, err)
}
