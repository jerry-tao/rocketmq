package rocketmq

import (
	"encoding/json"
	"testing"
)

func TestGetRouteInfoRequestHeader_MarshalJSON(t *testing.T) {
	h := &GetRouteInfoRequestHeader{
		topic:"topic",
	}
	res,err :=json.Marshal(h)
	t.Log(res)
	t.Log(string(res))
	t.Log(err)
	t.Log([]byte("中文"))
}
