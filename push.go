package bunnymq

import (
	"fmt"
)

// 处理消费者
const consumerProgressBucket = "consumer_progress"

// 定义 DBClientInterface 接口
type DBClientInterface interface {
	CreateBucket(bucketName string) error
	Get(bucketName, key string) ([]byte, error)
	PutWithAutoIncrementKey(bucketName string, value []byte) error
	GetNext(bucketName string, progress int64) (*KeyValue, error)
	GetAll(bucketName string) ([]*KeyValue, error)
	CleanupAllConsumed() error
	backupAndReopen() error
	Delete(bucketName, key string) error
	Close() error

	GetFirst(bucketName string) (*KeyValue, error)
}

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
