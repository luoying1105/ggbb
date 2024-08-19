package pkg

import (
	"sync"
	"sync/atomic"
)

const dbName = "ggb.db"

var (
	clientRefCount int32
	clientMutex    sync.Mutex
)

// Queue encapsulates the operations to enqueue and dequeue messages in a queue.
type Queue[T any] struct {
	queueName       string
	coder           Coder[T]
	msgManager      *MessageStore[T]
	progressManager *ConsumerProgressManager
	mu              sync.Mutex // To ensure thread-safe operations
}

// NewQueue creates a new queue with the given database client, queue name, and coder.
func NewQueue[T any](queueName string, coder Coder[T]) (*Queue[T], error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	err := initDBClient()
	if err != nil {
		return nil, err
	}
	atomic.AddInt32(&clientRefCount, 1)

	// Initialize MessageStore and ConsumerProgressManager
	msgManager, err := NewMessageStore[T](globalDBClient, coder)
	if err != nil {
		return nil, err
	}
	progressManager := NewConsumerProgressManager(globalDBClient)
	return &Queue[T]{
		queueName:       queueName,
		coder:           coder,
		msgManager:      msgManager,
		progressManager: progressManager,
	}, nil
}

// Enqueue adds a new item to the queue.
func (q *Queue[T]) Enqueue(data T) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.msgManager.Write(q.queueName, data)
}

// Dequeue retrieves and removes the first item from the queue.
func (q *Queue[T]) Dequeue(consumerID string) (Msg[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Get the current progress for this consumer
	progress, err := q.progressManager.GetProgress(consumerID, q.queueName)
	if err != nil {
		return nil, err
	}
	// Retrieve the message based on the consumer's progress
	data, err := q.msgManager.Read(q.queueName, progress)
	if err != nil {
		return nil, err
	}

	// Return the message with Ack/NAck handling
	return &MsgImpl[T]{
		data:            data,
		queueName:       q.queueName,
		consumerID:      consumerID,
		progressManager: q.progressManager,
	}, nil
}

// CleanDB cleans up consumed messages.
func CleanDB() error {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	err := initDBClient()
	if err != nil {
		return err
	}
	return globalDBClient.CleanupAllConsumed()
}

func Close() error {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	if atomic.AddInt32(&clientRefCount, -1) == 0 {
		return globalDBClient.Close()
	}
	return nil
}
