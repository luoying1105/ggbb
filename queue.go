package bunnymq

import (
	"fmt"
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
	dbPath          string
	db              *dbClient
	coder           Coder[T]
	msgManager      *MessageStore[T]
	progressManager *ConsumerProgressManager
	mu              sync.Mutex // To ensure thread-safe operations
}

// NewQueue creates a new queue with the given database client, queue name, and coder.
func NewQueue[T any](queueName, dbPath string, coder Coder[T]) (*Queue[T], error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	db, err := newDBClient(dbPath)
	if err != nil {
		return nil, err
	}
	atomic.AddInt32(&clientRefCount, 1)

	// Initialize MessageStore and ConsumerProgressManager
	msgManager, err := NewMessageStore[T](db, coder)
	if err != nil {
		return nil, err
	}
	progressManager := NewConsumerProgressManager(db)
	return &Queue[T]{
		queueName:       queueName,
		db:              db,
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
	// 获取当前消费者的进度
	progress, err := q.progressManager.GetProgress(consumerID, q.queueName)
	if err != nil {
		return nil, err
	}

	// 根据进度读取消息
	data, err := q.msgManager.Read(q.queueName, fmt.Sprintf("%d", progress+1))
	if err != nil {
		return nil, err
	}

	// 返回消息，但不更新进度，进度更新在 Ack 时进行
	return &MsgImpl[T]{
		data:            data,
		queueName:       q.queueName,
		consumerID:      consumerID,
		progressManager: q.progressManager,
	}, nil
}

// CleanDB cleans up consumed messages.
func CleanDB(dbPath string) error {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	db, err := newDBClient(dbPath)
	if err != nil {
		return err
	}

	return db.CleanupAllConsumed()
}

func (q *Queue[T]) Close() error {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if atomic.AddInt32(&clientRefCount, -1) == 0 {
		return q.db.Close()
	}
	return nil
}
