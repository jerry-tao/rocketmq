package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"os"
	"time"
)

type messageClientID struct {
	startTime     int64
	nextStartTime int64
	base          []byte
}

func init() {
	var baseBuilder = bytes.NewBuffer([]byte{})

	binary.Write(baseBuilder, binary.BigEndian, GetLocalIp4()) // 4
	binary.Write(baseBuilder, binary.BigEndian, os.Getpid())   // 2
	MessageClientIDSetter.base = baseBuilder.Bytes()
	MessageClientIDSetter.setStartTime()
}

var MessageClientIDSetter = messageClientID{}

func (m messageClientID) setUniqID(msg *Message) {
	if msg.GetProperty(UniqKey) == "" {
		msg.SetProperty(UniqKey, m.createUniqID())
	}
}

func (m messageClientID) getUniqID(msg *Message) string {
	return msg.GetProperty(UniqKey)
}

func (m messageClientID) createUniqID() string {
	current := time.Now().UnixNano()
	if current > m.nextStartTime {
		m.setStartTime()
	}
	base := bytes.NewBuffer(m.base)
	binary.Write(base, binary.BigEndian, time.Now().UnixNano()-m.startTime)
	binary.Write(base, binary.BigEndian, rand.Int63())
	return hex.EncodeToString(base.Bytes())
}

func (m messageClientID) setStartTime() {
	m.startTime = time.Now().UnixNano()
	m.nextStartTime = time.Now().UnixNano() + 2592000000000000 // next 30 days, 3600 * 24 * 30 * 1000 * 1000 *1000
}
