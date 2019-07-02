package rocketmq

type permName struct {
	PermPriority int
	PermRead     int
	PermWrite    int
	PermInherit  int
}

var PermName = permName{
	PermPriority: 0x1 << 3,
	PermRead:     0x1 << 2,
	PermWrite:    0x1 << 1,
	PermInherit:  0x1 << 0,
}

func (p permName) isWritable(perm int) bool {
	return (perm & p.PermWrite) == p.PermWrite
}