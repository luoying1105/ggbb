package pkg

import (
	"encoding/json"
	"errors"
)

// 定义泛型接口
type MessageInterface[V any] interface {
	Read(bucketName string, progress int64) (*V, error)
	Write(bucketName string, value V) error
}

// 定义 MessageStore 结构体，实现泛型接口
type MessageStore[V any] struct {
	dbClient *DBClient
}

// 创建新的 MessageStore 实例
func NewMessageStore[V any](dbClient *DBClient) (*MessageStore[V], error) {
	// 创建 bucket 如果不存在
	err := dbClient.CreateBucket("consumer_progress")
	if err != nil {
		return nil, err
	}
	return &MessageStore[V]{dbClient: dbClient}, nil
}

// 实现 Write 方法
func (ms *MessageStore[V]) Write(bucketName string, value V) error {
	// 将泛型 value 序列化为字节数组
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	// 尝试存储字节数组
	err = ms.StoreByte(bucketName, data)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			// 如果 bucket 不存在，则创建 bucket
			err = ms.dbClient.CreateBucket(bucketName)
			if err != nil {
				return err
			}
			// 再次尝试存储字节数组
			err = ms.StoreByte(bucketName, data)
			if err != nil {
				return err
			}
		} else {
			// 处理其他错误
			//return fmt.Errorf("failed to store byte: %w", err)
			return err
		}
	}

	return nil
}

// 实现 Read 方法
func (ms *MessageStore[V]) Read(bucketName string, progress int64) (*V, error) {
	keyValue, err := ms.dbClient.GetNext(bucketName, progress)
	if err != nil {
		return nil, err
	}
	var value V
	// 将字节数组反序列化为泛型类型
	err = json.Unmarshal(keyValue.Value, &value)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// 私有方法，用于存储字节数组
func (ms *MessageStore[V]) StoreByte(bucketName string, message []byte) error {
	return ms.dbClient.PutWithAutoIncrementKey(bucketName, message)
}
