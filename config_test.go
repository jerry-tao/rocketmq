package rocketmq

import "testing"

func TestConfig(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{"Nil config", nil},
		{"Namesrv empty config", &Config{}},
		{"Group invalid", &Config{Namesrv: "127.0.0.1"}},
		{"Empty ip config", &Config{Namesrv: "127.0.0.1", Group: "good"}},
		{"Unset pullMsgsNums config", &Config{Namesrv: "127.0.0.1", Group: "good"}},
		{"Valid config", &Config{Namesrv: "127.0.0.1", Group: "good", ClientIP: "0.0.0.0", PullMaxMsgNums: 5}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validConfig(test.config)
			switch test.name {
			case "Nil config":
				if err == nil || err != ErrConfigNil {
					t.Errorf(test.name, " failed")
				}
			case "Group invalid":
				if err == nil || err != ErrGroup {
					t.Errorf(test.name, " failed")
				}
			case "Namesrv empty config":
				if err == nil || err != ErrNameSrv {
					t.Errorf(test.name, " failed")
				}
			case "Empty ip config":
				if err != nil || test.config.ClientIP != localIP.String() {
					t.Errorf(test.name, " failed")
				}
			case "Unset pullMsgsNums config":
				if err != nil || test.config.PullMaxMsgNums != defaultPullMaxMsgsNums {
					t.Errorf(test.name, " failed")
				}
			case "Valid config":
				if err != nil || test.config.ClientIP != "0.0.0.0" || test.config.Namesrv != "127.0.0.1" || test.config.PullMaxMsgNums != 5 {
					t.Errorf(test.name, " failed")
				}
			}

		})
	}
}
