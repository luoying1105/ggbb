package pkg

import (
	"errors"
	"strconv"
)

type ConsumerProgressManager struct {
	dbClient *DBClient
}

func NewConsumerProgressManager(dbClient *DBClient) *ConsumerProgressManager {
	return &ConsumerProgressManager{
		dbClient: dbClient,
	}
}

// GetProgress retrieves the progress of a consumer for a specific queue.
func (cpm *ConsumerProgressManager) GetProgress(consumerID, queueName string) (int64, error) {
	key := cpm.buildProgressKey(consumerID, queueName)
	value, err := cpm.dbClient.Get(consumerProgressBucket, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			// 没有找到与该消费者和队列相关的进度记录
			return 0, nil
		}
		return 0, err
	}
	if value == nil {
		return 0, nil
	}

	progress, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, ErrInvalidProgress
	}

	return progress, nil
}

// UpdateProgress updates the progress of a consumer for a specific queue.
func (cpm *ConsumerProgressManager) UpdateProgress(consumerID, queueName string, newProgress int64) error {
	key := cpm.buildProgressKey(consumerID, queueName)
	err := cpm.dbClient.Put(consumerProgressBucket, key, []byte(strconv.FormatInt(newProgress, 10)))
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			err := cpm.dbClient.CreateBucket(consumerProgressBucket)
			if err != nil {
				return err
			}
			return cpm.dbClient.Put(consumerProgressBucket, key, []byte(strconv.FormatInt(newProgress, 10)))
		}
		return err
	}
	return nil
}

// buildProgressKey constructs a unique key for storing consumer progress.
func (cpm *ConsumerProgressManager) buildProgressKey(consumerID, queueName string) string {
	return queueName + "_" + consumerID
}
