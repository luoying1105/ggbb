package bunnymq

import (
	"errors"
)

type MessageStore[V any] struct {
	dbClient *dbClient
	coder    Coder[V]
}

func NewMessageStore[V any](dbClient *dbClient, coder Coder[V]) (*MessageStore[V], error) {
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

func (ms *MessageStore[V]) Read(bucketName, progress string) (V, error) {

	keyValue, err := ms.dbClient.GetNext(bucketName, progress)
	if err != nil {
		var zero V
		return zero, err
	}
	if keyValue == nil {
		var zero V
		return zero, ErrKeyNotFound
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
