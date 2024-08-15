package main

import (
	"fmt"
	"gbblox/pkg"
	"log"
)

func main() {
	// Initialize the database client
	dbClient, err := pkg.NewDBClient("test_queue.db")
	if err != nil {
		log.Fatalf("Failed to initialize DBClient: %v", err)
	}
	defer dbClient.Close()

	// Initialize the message store and progress manager
	messageStore := pkg.NewMessageStore(dbClient)
	progressManager := pkg.NewConsumerProgressManager(dbClient)

	// Initialize the QueueManager and ConsumerManager
	queueManager := pkg.NewQueueManager(messageStore)
	consumerManager := pkg.NewConsumerManager(messageStore, progressManager)

	// Create a queue
	queueName := "testQueue"
	err = queueManager.CreateQueue(queueName)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	// Publish some messages to the queue
	for i := 1; i <= 5; i++ {
		message := fmt.Sprintf("Message %d", i)
		err = queueManager.PublishMessage(queueName, message)
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Simulate a consumer subscribing to the queue
	consumerID := "consumer5"
	for {
		kv, err := consumerManager.Subscribe(queueName, consumerID)
		if err != nil {
			log.Printf("Failed to subscribe to queue: %v", err)
			break
		}
		if kv == nil {
			fmt.Println("No more messages in the queue")
			break
		}

		fmt.Printf("Consumer %s received message: %s\n", consumerID, kv.String())
	}
}
