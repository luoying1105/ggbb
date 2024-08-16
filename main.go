package main

import (
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
	pkg.SetLoggerLevel(slog.LevelDebug)
	pkg.Logger.Info("Application started")

	var messages []testStruct
	for i := 1; i <= 30; i++ {
		messages = append(messages, testStruct{
			sid:     fmt.Sprintf("id_%d", i),
			Message: fmt.Sprintf("Message #%d", i),
			Time:    time.Now(),
		})
	}
	queue, err := pkg.NewQueue[testStruct]("struct_queue", &pkg.JsonCoder[testStruct]{})
	for _, msg := range messages {
		if err := queue.Enqueue(msg); err != nil {
			pkg.Logger.Error(fmt.Sprintf("Error: %v", err))
		}
		pkg.Logger.Debug(fmt.Sprintf("Published: %+v", msg))
	}

	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer queue.Close()
	var wg sync.WaitGroup
	consumerIDs := []string{"consumer1", "consumer2", "consumer3"}
	for _, consumerID := range consumerIDs {
		wg.Add(1)
		go func(consumerID string) {
			defer wg.Done()
			for {
				msg, err := queue.Dequeue(consumerID)
				if err != nil {
					pkg.Logger.Error(fmt.Sprintf("Error: %v", err))
					break
				}
				pkg.Logger.Info(fmt.Sprintf("Consumed by %s: %+v", consumerID, msg.Data()))
				if err := msg.Ack(); err != nil {
					pkg.Logger.Error(fmt.Sprintf("Error acknowledging message: %v", err))
				}
			}
		}(consumerID)
	}
	wg.Wait()
	pkg.Logger.Info("All consumers have finished processing.")
	queue.CleanDB()
}
