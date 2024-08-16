package pkg

import (
	"errors"
	"strconv"
	"sync/atomic"
)

type ConsumerProgressManagerInterface interface {
	GetProgress(consumerID, queueName string) (int64, error)
	UpdateProgress(consumerID, queueName string, progress int64) error
}

type ConsumerProgressManager struct {
	dbClient    *DBClient
	progress    int64 // 使用 atomic 操作的进度值
	initialized int32 // 标志进度是否初始化
}

func NewConsumerProgressManager(dbClient *DBClient) *ConsumerProgressManager {
	return &ConsumerProgressManager{
		dbClient:    dbClient,
		initialized: 0,
	}
}

// GetProgress retrieves the progress of a consumer for a specific queue.
func (cpm *ConsumerProgressManager) GetProgress(consumerID, queueName string) (int64, error) {
	// 如果还没有初始化，加载并设置进度值
	if atomic.LoadInt32(&cpm.initialized) == 0 {
		key := cpm.buildProgressKey(consumerID, queueName)
		value, err := cpm.dbClient.Get(consumerProgressBucket, key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				// 没有找到与该消费者和队列相关的 消费者第一次访问队列时是预期的，因为此时还没有记录任何进度
				Logger.Debug(" Key not found, return 0 as the initial progress")
				return 0, nil
			}
			return 0, err
		}
		//函数默认返回 0 作为初始进度， 消费者将从队列的开头开始读取
		if value == nil {
			return 0, nil
		}

		progress, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, ErrInvalidProgress
		}

		atomic.StoreInt64(&cpm.progress, progress)
		atomic.StoreInt32(&cpm.initialized, 1) // 标记初始化完成
	}

	// 返回当前的进度值
	return atomic.LoadInt64(&cpm.progress), nil
}

// UpdateProgress updates the progress of a consumer for a specific queue.
func (cpm *ConsumerProgressManager) UpdateProgress(consumerID, queueName string, newProgress int64) error {
	atomic.StoreInt64(&cpm.progress, newProgress)

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
