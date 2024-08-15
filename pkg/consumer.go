package pkg

import (
	"fmt"
	"strconv"
)

type ConsumerProgressManagerInterface interface {
	GetProgress(consumerID, queueName string) (int64, error)
	UpdateProgress(consumerID, queueName string, progress int64) error
}

// 处理消费者
const consumerProgressBucket = "consumer_progress"

type ConsumerProgressManager struct {
	dbClient *DBClient
}

func NewConsumerProgressManager(dbClient *DBClient) *ConsumerProgressManager {
	return &ConsumerProgressManager{dbClient: dbClient}
}

// GetProgress retrieves the progress of a consumer for a specific queue.
func (cpm *ConsumerProgressManager) GetProgress(consumerID, queueName string) (int64, error) {
	key := cpm.buildProgressKey(consumerID, queueName)

	value, err := cpm.dbClient.Get(consumerProgressBucket, key)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}

	progress, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse progress value: %v", err)
	}
	return progress, nil
}

// UpdateProgress updates the progress of a consumer for a specific queue.
func (cpm *ConsumerProgressManager) UpdateProgress(consumerID, queueName string, progress int64) error {
	key := cpm.buildProgressKey(consumerID, queueName)
	return cpm.dbClient.Put(consumerProgressBucket, key, strconv.FormatInt(progress, 10))
}

// buildProgressKey constructs a unique key for storing consumer progress.
func (cpm *ConsumerProgressManager) buildProgressKey(consumerID, queueName string) string {
	return queueName + "_" + consumerID
}
