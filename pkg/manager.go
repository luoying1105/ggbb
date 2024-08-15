package pkg

// QueueManager handles the creation of queues and publishing messages.
type QueueManager[V any] struct {
	store *MessageStore[V]
}

// NewQueueManager creates a new QueueManager for a specific message type.
func NewQueueManager[V any](store *MessageStore[V]) *QueueManager[V] {
	return &QueueManager[V]{store: store}
}

// CreateQueue creates a new queue with the given name.
func (qm *QueueManager[V]) CreateQueue(queueName string) error {
	return qm.store.dbClient.CreateBucket(queueName)
}

// PublishMessage publishes a message to the specified queue.
func (qm *QueueManager[V]) PublishMessage(queueName string, message V) error {
	return qm.store.Write(queueName, message)
}

// ConsumerManager handles subscription and message retrieval for consumers.
type ConsumerManager[V any] struct {
	store    *MessageStore[V]
	progress *ConsumerProgressManager
}

// NewConsumerManager creates a new ConsumerManager for a specific message type.
func NewConsumerManager[V any](store *MessageStore[V], progress *ConsumerProgressManager) *ConsumerManager[V] {
	return &ConsumerManager[V]{store: store, progress: progress}
}

// Subscribe subscribes a consumer to a queue and retrieves the next message.
func (cm *ConsumerManager[V]) Subscribe(queueName, consumerID string) (*V, error) {
	progress, err := cm.progress.GetProgress(consumerID, queueName)
	if err != nil {
		// 如果未找到该消费者的进度，则初始化
		if dbErr, ok := err.(*DBError); ok && dbErr.Err == ErrKeyNotFound {
			progress = 0 // 初始化进度为 0
			err = cm.progress.UpdateProgress(consumerID, queueName, progress)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	message, err := cm.store.Read(queueName, progress)
	if err != nil {
		return nil, err
	}

	if message != nil {
		err = cm.progress.UpdateProgress(consumerID, queueName, progress+1)
		if err != nil {
			return nil, err
		}
	}

	return message, nil
}
