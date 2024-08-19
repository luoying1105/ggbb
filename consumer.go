package bunnymq

import (
	"errors"
	"fmt"
	"strconv"
)

type consumerProgressManager struct {
	dbClient *dbClient
}

func newConsumerProgressManager(dbClient *dbClient) *consumerProgressManager {
	dbClient.ensureBucketExists(consumerProgressBucket)
	return &consumerProgressManager{
		dbClient: dbClient,
	}
}

// GetProgress retrieves the progress of a consumer for a specific queue.
func (cpm *consumerProgressManager) getProgress(consumerID, queueName string) (int64, error) {
	key := cpm.buildProgressKey(consumerID, queueName)
	value, err := cpm.dbClient.get(consumerProgressBucket, key)
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
func (cpm *consumerProgressManager) updateProgress(consumerID, queueName string, newProgress int64) error {
	key := cpm.buildProgressKey(consumerID, queueName)
	return cpm.dbClient.put(consumerProgressBucket, key, []byte(fmt.Sprintf("%d", newProgress)))
}

// buildProgressKey constructs a unique key for storing consumer progress.
func (cpm *consumerProgressManager) buildProgressKey(consumerID, queueName string) string {
	return fmt.Sprintf("%s:%s", consumerID, queueName)
}
