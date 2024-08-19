package bunnymq

import (
	"fmt"
)

// 处理消费者
const consumerProgressBucket = "consumer_progress"

type KeyValue struct {
	Key   string
	Value []byte
}

func (kv KeyValue) String() string {
	return fmt.Sprintf("Key: %s, Value: %s", kv.Key, string(kv.Value))
}

type Options struct {
	Queue   string
	Durable bool
	AutoAck bool
}
