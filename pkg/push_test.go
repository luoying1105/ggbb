package pkg

import (
	"fmt"

	"testing"
	"time"
)

type testStruct struct {
	sid     string
	Message string
	Time    time.Time
}

// 验证 多个队列推送 并且只有一个消费者
func TestMultiQueue1(t *testing.T) {
	queueNames := []string{"queue1", "queue2", "queue3"}
	defer Close()
	var queues []*Queue[testStruct]
	for _, queueName := range queueNames {
		queue, err := NewQueue[testStruct](queueName, &JsonCoder[testStruct]{})
		if err != nil {
			t.Fatalf("Error creating queue %s: %v", queueName, err)
		}
		queues = append(queues, queue)

	}

	// Push 30 messages to each queue
	for i, queue := range queues {
		num := 0
		for j := 1; j <= 30; j++ {
			msg := testStruct{
				sid:     fmt.Sprintf("Queue%d", i+1),
				Message: fmt.Sprintf("Message %d", j),
				Time:    time.Now(),
			}
			err := queue.Enqueue(msg)
			if err != nil {
				t.Fatalf("Error enqueuing message to %s: %v", queueNames[i], err)
			}
			num = 1 + num
		}
		t.Logf("%s 推送 %d 信息", queue.queueName, num)

	}

	// Consume messages only from the first queue
	consumerID := "consumer2"
	receivedMessages := 0
	for {
		msg, err := queues[0].Dequeue(consumerID)
		if err != nil {
			break
		}
		receivedMessages++
		if err := msg.Ack(); err != nil {
			t.Errorf("Error acknowledging message: %v", err)
		}
	}

	t.Logf("%s %s 消费了 %d  ", queues[0].queueName, consumerID, receivedMessages)

	if receivedMessages != 30 {
		t.Errorf("Consumer %s did not receive all messages from %s, received: %d", consumerID, queueNames[0], receivedMessages)
	}

	// Validate that other queues have all their messages (should be 30 each)
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
		}
	}

}
