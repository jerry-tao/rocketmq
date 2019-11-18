package rocketmq

import (
	"bytes"
	"errors"
	"testing"
)

func TestDecoder(t *testing.T) {

	b2 := []byte{0, 0, 0, 192, 0, 0, 0, 109, 123, 34, 99, 111, 100, 101, 34, 58, 48, 44, 34, 108, 97, 110, 103, 117, 97, 103, 101, 34, 58, 34, 74, 65, 86, 65, 34, 44, 34, 118, 101, 114, 115, 105, 111, 110, 34, 58, 50, 57, 51, 44, 34, 111, 112, 97, 113, 117, 101, 34, 58, 49, 48, 48, 54, 44, 34, 102, 108, 97, 103, 34, 58, 49, 44, 34, 114, 101, 109, 97, 114, 107, 34, 58, 34, 34, 44, 34, 101, 120, 116, 70, 105, 101, 108, 100, 115, 34, 58, 123, 34, 111, 102, 102, 115, 101, 116, 34, 58, 34, 51, 48, 51, 53, 54, 56, 34, 125, 125, 123, 34, 108, 111, 99, 107, 79, 75, 77, 81, 83, 101, 116, 34, 58, 91, 123, 34, 98, 114, 111, 107, 101, 114, 78, 97, 109, 101, 34, 58, 34, 114, 111, 99, 107, 101, 116, 109, 113, 49, 34, 44, 34, 113, 117, 101, 117, 101, 73, 100, 34, 58, 52, 50, 44, 34, 116, 111, 112, 105, 99, 34, 58, 34, 115, 116, 97, 98, 108, 101, 95, 116, 101, 115, 116, 34, 125, 93, 125}
	r2 := bytes.NewBuffer(b2)
	der2 := newDecoder(r2)
	var cmd RemotingCommand
	err := der2.Decode(&cmd)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

}

type mockReader string

var boom = errors.New("boom")

func (*mockReader) Read(b []byte) (n int, err error) {
	return 0, boom
}


func TestDecoderError(t *testing.T) {
	der2 := newDecoder(new(mockReader))
	var cmd RemotingCommand
	err := der2.Decode(&cmd)
	if err != boom {
		t.Log(err)
		t.Fail()
	}
}
