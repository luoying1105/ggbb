package pkg

import (
	"errors"
)

type MessageStore[V any] struct {
	dbClient *DBClient
	coder    Coder[V]
}

func NewMessageStore[V any](dbClient *DBClient, coder Coder[V]) (*MessageStore[V], error) {
	err := dbClient.CreateBucket("consumer_progress")
	if err != nil {
		return nil, err
	}
	return &MessageStore[V]{dbClient: dbClient, coder: coder}, nil
}

func (ms *MessageStore[V]) Write(bucketName string, value V) error {
	// Encode the value using the coder
	data, err := ms.coder.Encode(value)
	if err != nil {
		return err
	}

	err = ms.StoreByte(bucketName, data)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			err = ms.dbClient.CreateBucket(bucketName)
			if err != nil {
				return err
			}
			err = ms.StoreByte(bucketName, data)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func (ms *MessageStore[V]) Read(bucketName string, progress int64) (V, error) {
	keyValue, err := ms.dbClient.GetNext(bucketName, progress)
	if err != nil {
		var zero V
		return zero, err
	}
	// Decode the data using the coder
	value, err := ms.coder.Decode(keyValue.Value)
	if err != nil {
		var zero V
		return zero, err
	}
	return value, nil
}

func (ms *MessageStore[V]) StoreByte(bucketName string, message []byte) error {
	return ms.dbClient.PutWithAutoIncrementKey(bucketName, message)
}
