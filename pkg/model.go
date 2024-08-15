package pkg

import "fmt"

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
