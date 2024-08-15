package main

import (
	"fmt"
	"gbblox/pkg"
	"log"
)

func main() {
	// 初始化数据库客户端
	dbClient, err := pkg.NewDBClient("test.db")
	if err != nil {
		log.Fatalf("Failed to create DB client: %v", err)
	}
	defer dbClient.Close()

	// 初始化消费者进度管理器
	progressManager := pkg.NewConsumerProgressManager(dbClient)

	// 初始化字符串类型的 MessageStore
	stringStore, err := pkg.NewMessageStore[string](dbClient)
	if err != nil {
		log.Fatalf("Failed to create MessageStore: %v", err)
	}

	// 队列名称和消费者 ID
	queueName := "test_queue"
	consumerID := "consumer2"

	//// 发布消息
	//for i := 1; i <= 10; i++ {
	//	message := fmt.Sprintf("Message %d", i)
	//	err := stringStore.Write(queueName, message)
	//	if err != nil {
	//		log.Printf("Failed to publish message: %v", err)
	//	}
	//}

	// 消费消息
	for {
		// 获取当前消费者的进度
		progress, err := progressManager.GetProgress(consumerID, queueName)
		if err != nil {
			log.Printf("Failed to get progress: %v", err)
			break
		}

		// 获取下一条消息
		message, err := stringStore.Read(queueName, progress)
		if err != nil {
			if err == pkg.ErrNoMoreMessages {
				log.Println("No more messages to consume.")
				break
			} else {
				log.Printf("Failed to read message: %v", err)
				break
			}
		}

		// 处理消息（这里只是打印出来）
		fmt.Printf("Consumed message: %s\n", *message)

		// 更新消费者的进度
		err = progressManager.UpdateProgress(consumerID, queueName, progress+1)
		if err != nil {
			log.Printf("Failed to update progress: %v", err)
			break
		}
	}
	log.Printf("开始清理已经消费过的消息")
	// 清理所有已消费的消息
	err = dbClient.CleanupAllConsumed()
	if err != nil {
		log.Printf("Failed to cleanup consumed messages: %v", err)
	} else {
		log.Println("Successfully cleaned up all consumed messages.")
	}
}
