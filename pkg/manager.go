package pkg

type QueueManager struct {
	store *MessageStore
}

func NewQueueManager(store *MessageStore) *QueueManager {
	return &QueueManager{store: store}
}

func (qm *QueueManager) CreateQueue(queueName string) error {
	return qm.store.dbClient.CreateBucket(queueName)
}

func (qm *QueueManager) PublishMessage(queueName, message string) error {
	return qm.store.StoreMessage(queueName, message)
}

type ConsumerManager struct {
	store    *MessageStore
	progress *ConsumerProgressManager
}

func NewConsumerManager(store *MessageStore, progress *ConsumerProgressManager) *ConsumerManager {
	return &ConsumerManager{store: store, progress: progress}
}

func (cm *ConsumerManager) Subscribe(queueName, consumerID string) (*KeyValue, error) {
	progress, err := cm.progress.GetProgress(consumerID, queueName)
	if err != nil {
		return nil, err
	}

	kv, err := cm.store.RetrieveMessage(queueName, progress)
	if err != nil {
		return nil, err
	}

	if kv != nil {
		err = cm.progress.UpdateProgress(consumerID, queueName, progress+1)
		if err != nil {
			return nil, err
		}
	}

	return kv, nil
}
