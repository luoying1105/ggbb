package main

import (
	"errors"
	"fmt"
	"gbblox/pkg"
	"log"
	"log/slog"
	"sync"
	"time"
)

type testStruct struct {
	sid     string
	Message string
	Time    time.Time
}

func main() {
	// 初始化时将日志几倍修改为debug
	// 初始化时将日志级别设置为 Debug
	pkg.SetLoggerLevel(slog.LevelDebug)
	pkg.Logger.Info("Application started")

	dbClient, err := createDBClient("test.db")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer dbClient.Close()

	structStore, err := pkg.NewMessageStore[testStruct](dbClient)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	pkg.Logger.Info("数据推送")
	var messages []testStruct
	for i := 1; i <= 130; i++ {
		messages = append(messages, testStruct{
			sid:     fmt.Sprintf("id_%d", i),
			Message: fmt.Sprintf("Message #%d", i),
			Time:    time.Now(),
		})
	}
	// Publish the generated messages
	if err := publishMessages(structStore, "struct_queue", messages); err != nil {
		pkg.Logger.Error(fmt.Sprintf("Error: %v", err))
	}

	// 启动三个消费者进行消费
	var wg sync.WaitGroup
	progressManager := pkg.NewConsumerProgressManager(dbClient)
	consumerIDs := []string{"consumer1", "consumer2", "consumer6"}
	for _, consumerID := range consumerIDs {
		wg.Add(1)
		go func(consumerID string) {
			defer wg.Done()
			pkg.Logger.Info(fmt.Sprintf("Consumer %s 开始消费", consumerID))
			if err := consumeMessages(structStore, progressManager, consumerID, "struct_queue"); err != nil {
				pkg.Logger.Error(fmt.Sprintf("Error: %v", err))
			}
		}(consumerID)
	}

	wg.Wait()

	pkg.Logger.Info("所有消费者已完成消费")

	pkg.Logger.Info("清理所有数据")
	if err := cleanupConsumedMessages(dbClient); err != nil {
		fmt.Println(err)
	}

}

// 发布消息
func publishMessages[V any](store *pkg.MessageStore[V], queueName string, messages []V) error {
	for _, msg := range messages {
		if err := store.Write(queueName, msg); err != nil {
			return err
		}
		pkg.Logger.Debug(fmt.Sprintf("Published: %+v", msg))
	}
	return nil
}

// 订阅并消费消息
func consumeMessages[V any](store *pkg.MessageStore[V], progressManager *pkg.ConsumerProgressManager, consumerID, queueName string) error {
	for {
		progress, err := progressManager.GetProgress(consumerID, queueName)
		if err != nil {
			return err
		}

		msg, err := store.Read(queueName, progress)
		if err != nil {
			if errors.Is(err, pkg.ErrNoMoreMessages) {
				pkg.Logger.Info("No more messages to consume.")
				break
			}
			return err
		}

		pkg.Logger.Info(fmt.Sprintf("Consumed: %+v", *msg))
		if err := progressManager.UpdateProgress(consumerID, queueName, progress+1); err != nil {
			return err
		}
	}
	return nil
}

// 清理已消费消息
func cleanupConsumedMessages(dbClient *pkg.DBClient) error {
	if dbClient == nil {
		return errors.New("dbClient is nil")
	}
	pkg.Logger.Info("Starting cleanup of consumed messages.")
	pkg.Logger.Info("Before cleanup.")
	err := dbClient.CleanupAllConsumed()
	pkg.Logger.Info("After cleanup.")
	if err != nil {
		return err
	}

	pkg.Logger.Info("Successfully cleaned up all consumed messages.")
	return nil
}

// 创建数据库客户端
func createDBClient(dbPath string) (*pkg.DBClient, error) {
	dbClient, err := pkg.NewDBClient(dbPath)
	if err != nil {
		return nil, err
	}
	return dbClient, nil
}
