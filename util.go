package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"net"
	"os"
	"time"
)

var localIP = getLocalIP4()
var idGen []byte

func init() {
	rand.Seed(time.Now().UnixNano())
	var baseBuilder = bytes.NewBuffer([]byte{})
	baseBuilder.Write(localIP)
	binary.Write(baseBuilder, binary.BigEndian, int16(os.Getpid())) // 2
	idGen = baseBuilder.Bytes()
}

func getLocalIP4() net.IP {
	addresses, err := net.InterfaceAddrs()

	if err != nil {
		return []byte{0, 0, 0, 0}
	}

	for _, address := range addresses {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.To4()
			}
		}
	}
	return []byte{0, 0, 0, 0}
}

func genUniqKey() string {
	base := bytes.NewBuffer(idGen)
	binary.Write(base, binary.BigEndian, time.Now().UnixNano())
	binary.Write(base, binary.BigEndian, rand.Int63())
	return hex.EncodeToString(base.Bytes())
}

func varintInt(b []byte) int {
	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
	return int(uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24)
}
