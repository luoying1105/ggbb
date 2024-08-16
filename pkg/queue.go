package pkg

import (
	"sync"
)

const dbName = "ggb.db"

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
	// Internally, create and manage the DBClient.
	dbClient, err := NewDBClient(dbName)
	if err != nil {
		return nil, err
	}
	// Initialize MessageStore and ConsumerProgressManager
	msgManager, err := NewMessageStore[T](dbClient, coder)
	if err != nil {
		return nil, err
	}
	progressManager := NewConsumerProgressManager(dbClient)
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

// Close cleans up resources by closing the database connection.
func (q *Queue[T]) Close() error {
	return q.msgManager.dbClient.Close()
}

// generateKey generates a unique key for each item in the queue.
func (q *Queue[T]) CleanDB() error {
	return q.msgManager.dbClient.CleanupAllConsumed()
}
