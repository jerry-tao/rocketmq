package rocketmq

import (
	"bytes"
	"testing"
)

func TestDecoder(t *testing.T) {
	 b:=[]byte{0, 0, 0, 119, 0, 0, 0, 115, 123, 34, 99, 111, 100, 101, 34, 58, 49, 48, 53, 44, 34, 108, 97, 110, 103, 117, 97, 103, 101, 34, 58, 34, 71, 79, 76, 65, 78, 71, 34, 44, 34, 118, 101, 114, 115, 105, 111, 110, 34, 58, 55, 57, 44, 34, 111, 112, 97, 113, 117, 101, 34, 58, 49, 48, 48, 49, 44, 34, 102, 108, 97, 103, 34, 58, 48, 44, 34, 114, 101, 109, 97, 114, 107, 34, 58, 34, 34, 44, 34, 101, 120, 116, 70, 105, 101, 108, 100, 115, 34, 58, 123, 34, 116, 111, 112, 105, 99, 34, 58, 34, 116, 101, 115, 116, 95, 116, 111, 112, 105, 99, 34, 125, 125,0, 0, 0, 119, 0, 0, 0, 115, 123, 34, 99, 111, 100, 101, 34, 58, 49, 48, 53, 44, 34, 108, 97, 110, 103, 117, 97, 103, 101, 34, 58, 34, 71, 79, 76, 65, 78, 71, 34, 44, 34, 118, 101, 114, 115, 105, 111, 110, 34, 58, 55, 57, 44, 34, 111, 112, 97, 113, 117, 101, 34, 58, 49, 48, 48, 49, 44, 34, 102, 108, 97, 103, 34, 58, 48, 44, 34, 114, 101, 109, 97, 114, 107, 34, 58, 34, 34, 44, 34, 101, 120, 116, 70, 105, 101, 108, 100, 115, 34, 58, 123, 34, 116, 111, 112, 105, 99, 34, 58, 34, 116, 101, 115, 116, 95, 116, 111, 112, 105, 99, 34, 125, 125,0, 0, 0, 119, 0, 0, 0, 115, 123, 34, 99, 111, 100, 101, 34, 58, 49, 48, 53, 44, 34, 108, 97, 110, 103, 117, 97, 103, 101, 34, 58, 34, 71, 79, 76, 65, 78, 71, 34, 44, 34, 118, 101, 114, 115, 105, 111, 110, 34, 58, 55, 57, 44, 34, 111, 112, 97, 113, 117, 101, 34, 58, 49, 48, 48, 49, 44, 34, 102, 108, 97, 103, 34, 58, 48, 44, 34, 114, 101, 109, 97, 114, 107, 34, 58, 34, 34, 44, 34, 101, 120, 116, 70, 105, 101, 108, 100, 115, 34, 58, 123, 34, 116, 111, 112, 105, 99, 34, 58, 34, 116, 101, 115, 116, 95, 116, 111, 112, 105, 99, 34, 125, 125,}
	r := bytes.NewBuffer(b)
	der := newDecoder(r)
	var cmd,cmd1,cmd2 RemotingCommand
	err := der.Decode(&cmd)
	err = der.Decode(&cmd1)
	err = der.Decode(&cmd2)
	if err!=nil{
		t.Log(err)
		t.Fail()
	}
}

//TODO test with body