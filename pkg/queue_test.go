package pkg

import (
	"fmt"

	"sync"
	"testing"
	"time"
)

// 测试nack
func TestQueueWithNack(t *testing.T) {
	// 创建队列
	queue, err := NewQueue[testStruct]("test_queue_nack", &JsonCoder[testStruct]{})
	if err != nil {
		t.Fatalf("Error creating queue: %v", err)
	}
	defer Close()

	// 推送消息到队列
	message := testStruct{
		sid:     t.Name(),
		Message: "Message to be Nack",
		Time:    time.Now(),
	}

	if err := queue.Enqueue(message); err != nil {
		t.Fatalf("Error enqueuing message: %v", err)
	}

	consumerID := "consumer_nack"
	msg, err := queue.Dequeue(consumerID)
	if err != nil {
		t.Fatalf("Error dequeuing message: %v", err)
	}

	// Simulate processing failure and call Nack
	if err := msg.NAck(); err != nil {
		t.Errorf("Error calling Nack: %v", err)
	}

	// Dequeue the message again, it should be the same message
	msgAgain, err := queue.Dequeue(consumerID)
	if err != nil {
		t.Fatalf("Error dequeuing message again: %v", err)
	}
	t.Log(message.Message, message.Time)
	t.Log(msgAgain.Data().Message, message.Time)
	if msgAgain.Data().Message != message.Message {

		t.Errorf("Expected message: %s, got: %s", message.Message, msgAgain.Data().Message)
	}
}

// 一个队列不同消费者同时消费
func TestQueueMultipleConsumers(t *testing.T) {
	// 创建队列
	queue, err := NewQueue[testStruct]("test_queue", &JsonCoder[testStruct]{})
	if err != nil {
		t.Fatalf("Error creating queue: %v", err)
	}

	// 推送消息到队列
	var messages []testStruct
	for i := 1; i <= 30; i++ {
		messages = append(messages, testStruct{
			sid:     t.Name(),
			Message: fmt.Sprintf("Message %d", i),
			Time:    time.Now(),
		})
	}

	for _, msg := range messages {
		if err := queue.Enqueue(msg); err != nil {
			t.Fatalf("Error enqueuing message: %v", err)
		}
	}

	// 设置消费者数量
	consumerIDs := []string{"consumer1", "consumer2", "consumer3"}
	expectedMessages := len(messages)

	// 并发测试
	var wg sync.WaitGroup
	for _, consumerID := range consumerIDs {
		wg.Add(1)
		go func(consumerID string) {
			defer wg.Done()
			receivedMessages := 0
			for {
				msg, err := queue.Dequeue(consumerID)
				if err != nil {
					break
				}
				receivedMessages++
				// 不删除消息，只记录进度
				if err := msg.Ack(); err != nil {
					t.Errorf("Error acknowledging message: %v", err)
				}
			}

			t.Logf("Consumer %s received %d messages.", consumerID, receivedMessages)

			if receivedMessages != expectedMessages {
				t.Errorf("Consumer %s did not receive all messages, received: %d", consumerID, receivedMessages)
			}
		}(consumerID)
	}

	wg.Wait()
}

// 不同队列，只有一个消费者消费了一条队列
// Cleanup the database before running the test
func cleanDatabase() {
	err := CleanDB()
	if err != nil {
		fmt.Printf("Error cleaning database: %v\n", err)
	}
}

func TestMultiQueue(t *testing.T) {
	defer cleanDatabase()
	defer Close()
	// Define queue names
	queueNames := []string{"queue1", "queue2", "queue3"}

	// Create queues
	var queues []*Queue[testStruct]
	for _, queueName := range queueNames {
		queue, err := NewQueue[testStruct](queueName, &JsonCoder[testStruct]{})
		if err != nil {
			t.Fatalf("Error creating queue %s: %v", queueName, err)
		}
		queues = append(queues, queue)

	}

	// Push messages to different queues
	var wg sync.WaitGroup
	for i, queue := range queues {
		wg.Add(1)
		go func(queue *Queue[testStruct], idx int) {
			defer wg.Done()
			for j := 1; j <= 30; j++ {
				msg := testStruct{
					sid:     fmt.Sprintf("Queue%d", idx+1),
					Message: fmt.Sprintf("Message %d", j),
					Time:    time.Now(),
				}
				if err := queue.Enqueue(msg); err != nil {
					t.Fatalf("Error enqueuing message to %s: %v", queueNames[idx], err)
				}

			}
		}(queue, i)
	}
	wg.Wait()

	// Consume messages only from the first queue
	consumerID := "consumer1"
	queueToConsume := queues[0]
	receivedMessages := 0

	for {
		msg, err := queueToConsume.Dequeue(consumerID)
		if err != nil {
			break
		}
		receivedMessages++
		if err := msg.Ack(); err != nil {
			t.Errorf("Error acknowledging message: %v", err)
		}
	}

	// Check if all messages were consumed from queue1
	if receivedMessages != 30 {
		t.Errorf("Consumer %s did not receive all messages from %s, received: %d", consumerID, queueNames[0], receivedMessages)
	} else {
		t.Logf("Consumer %s successfully received all 30 messages from %s.", consumerID, queueNames[0])
	}

	// Validate that other queues have all their messages (30 each)
	for i, queue := range queues[1:] {
		consumerID := fmt.Sprintf("consumer%d", i+2)
		receivedMessages := 0

		for {
			msg, err := queue.Dequeue(consumerID)
			if err != nil {
				break
			}
			receivedMessages++
			if err := msg.Ack(); err != nil {
				t.Errorf("Error acknowledging message: %v", err)
			}
		}

		if receivedMessages != 30 {
			t.Errorf("Consumer %s did not receive all messages from %s, received: %d", consumerID, queueNames[i+1], receivedMessages)
		} else {
			t.Logf("Consumer %s successfully received all 30 messages from %s.", consumerID, queueNames[i+1])
		}
	}
}
