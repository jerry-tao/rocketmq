package rocketmq

import "testing"

func Test_getLocalIp4(t *testing.T) {
	ip := getLocalIP4()
	if len(ip) != 4 || ip.String() == "" {
		t.Fatal("Get local ip failed")
	}
	if len(localIP) != 4 || localIP.String() == "" {
		t.Fatal("Set local ip failed")
	}

}

func TestNewUniqKey(t *testing.T) {
	key := genUniqKey()
	t.Log(len(key)) // 44 too long
	if key == "" {
		t.Log("UniqKey fail")
	}
	repeat := map[string]bool{}
	for i := 0; i < 10000; i++ {
		key = genUniqKey()
		if _, ok := repeat[key]; ok {
			t.Fatal("UniqKey repeated")
		} else {
			repeat[key] = true
		}
	}

}
