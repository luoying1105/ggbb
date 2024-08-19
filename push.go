package bunnymq

import (
	"fmt"
)

// 处理消费者
const consumerProgressBucket = "consumer_progress"

type keyValue struct {
	key   string
	value []byte
}

func (kv keyValue) String() string {
	return fmt.Sprintf("key: %s, Value: %s", kv.key, string(kv.value))
}

type Options struct {
	Queue   string
	Durable bool
	AutoAck bool
}
