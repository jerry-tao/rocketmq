package rocketmq

import (
	"testing"
)

func TestDefaultRemotingClient_InvokeSync(t *testing.T) {
	cli := NewDefaultRemotingClient()
	cli.Start()
	requestHeader := &GetRouteInfoRequestHeader{
		topic: "tqa1",
	}

	remotingCommand := buildCommand(GetRouteinfoByTopic, requestHeader, nil)
	res, err := cli.InvokeSync("172.17.5.201:9876", remotingCommand, 1000)
	t.Log(res)
	t.Log(err)
}

func TestDefaultRemotingClient_InvokeAsync(t *testing.T) {
	cli := NewDefaultRemotingClient()
	cli.Start()
	requestHeader := &GetRouteInfoRequestHeader{
		topic: "tqa1",
	}
	ch := make(chan struct{})
	remotingCommand := buildCommand(GetRouteinfoByTopic, requestHeader, nil)
	err := cli.InvokeAsync("172.17.5.201:9876", remotingCommand, 1000, func(responseFuture *ResponseFuture) {
		t.Log(responseFuture)
		close(ch)
	})
	t.Log(err)
	<-ch
}

func TestDefaultRemotingClient_InvokeOneway(t *testing.T) {
	cli := NewDefaultRemotingClient()
	cli.Start()
	requestHeader := &GetRouteInfoRequestHeader{
		topic: "tqa1",
	}
	remotingCommand := buildCommand(GetRouteinfoByTopic, requestHeader, nil)
	err := cli.InvokeOneway("172.17.5.201:9876", remotingCommand, 1000)
	t.Log(err)
}
