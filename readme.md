### Bunnymq 使用指南

#### 1. 项目简介

`Bunnymq` 是一个基于 BoltDB 的消息队列库，提供了消息的推送（生产）、消费（订阅）、消息的确认（ACK）、拒绝（NACK），以及对已消费消息的清理功能。

#### 2. 如何安装

```bash
go env -w GOPRIVATE=gitlab.cnns 
go get gitlab.cnns/luoying/bunnymq@v0.0.1 
```

#### 3. 如何使用

##### 3.1 推送消息

```go
package main

import (
	"fmt"
	bunnymq "gitlab.cnns/luoying/bunnymq/pkg"
	"time"
)

type testStruct struct {
	sid     string
	Message string
	Time    time.Time
}

func main() {
	queueNames := []string{"queue1", "queue2", "queue3"}
	var queues []*bunnymq.Queue[testStruct]
	for _, queueName := range queueNames {
		queue, err := bunnymq.NewQueue[testStruct](queueName, &bunnymq.JsonCoder[testStruct]{})
		if err != nil {
			fmt.Printf("Error creating queue %s: %v", queueName, err)
		}
		queues = append(queues, queue)
	}

	// 往队列中推送消息
	for i, queue := range queues {
		for j := 1; j <= 30; j++ {
			msg := testStruct{
				sid:     fmt.Sprintf("Queue%d", i+1),
				Message: fmt.Sprintf("Message %d", j),
				Time:    time.Now(),
			}
			err := queue.Enqueue(msg)
			if err != nil {
				fmt.Printf("Error enqueuing message to %s: %v", queueNames[i], err)
			}
		}
	}
}
```

##### 3.2 消费消息并确认（ACK）

```go
consumerID := "consumer1"
receivedMessages := 0
for {
msg, err := queues[0].Dequeue(consumerID)
if err != nil {
break
}
receivedMessages++
// 确认消息
if err := msg.Ack(); err != nil {
fmt.Printf("Error acknowledging message: %v", err)
}
}
```

##### 3.3 消息拒绝（NACK）

```go
if err := msg.NAck(); err != nil {
fmt.Printf("Error rejecting message: %v", err)
}
```

##### 3.4 清理已消费的消息

```go
err := bunnymq.CleanDB()
if err != nil {
fmt.Println("Error cleaning database:", err)
}
```

##### 3.5 关闭数据库连接

```go
err := bunnymq.Close()
if err != nil {
fmt.Println("Error closing database:", err)
}
```

#### 4. 注意事项

- 确保每个消费者都有唯一的 `consumerID`。
- 在程序结束时，务必关闭数据库连接。

#### 5. 示例项目

```
package main

import (
	"fmt"
	bunnymq "gitlab.cnns/luoying/bunnymq"
	"time"
)

type testStruct struct {
	sid     string
	Message string
	Time    time.Time
}

func main() {
	queueNames := []string{"queue1", "queue2", "queue3"}
	// 创建并推送消息到多个队列
	var queues []*bunnymq.Queue[testStruct]
	for _, queueName := range queueNames {
		queue, err := bunnymq.NewQueue[testStruct](queueName, &bunnymq.JsonCoder[testStruct]{})
		if err != nil {
			fmt.Printf("Error creating queue %s: %v", queueName, err)
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
				fmt.Printf("Error enqueuing message to %s: %v", queueNames[i], err)
			}
			num = 1 + num
		}
		//fmt.Printf("%s 推送 %d 信息", queue.queueName, num)

	}

	// 消费消息并确认
	consumerID := "consumer2"
	receivedMessages := 0
	for {
		msg, err := queues[0].Dequeue(consumerID)
		if err != nil {
			break
		}
		receivedMessages++
		if err := msg.Ack(); err != nil {
			fmt.Printf("Error acknowledging message: %v", err)
		}
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
				fmt.Printf("Error acknowledging message: %v", err)
			}
		}

		if receivedMessages != 30 {
			fmt.Println(fmt.Sprintf("Consumer %s did not receive all messages from %s, received: %d", consumerID, queueNames[i+1], receivedMessages))

		}
	}

	//关闭数据库
	err := bunnymq.Close()
	if err != nil {
		fmt.Println(err)
	}

	//清理所有已经推送数据
	err = bunnymq.CleanDB()
	if err != nil {
		fmt.Println(err)
	}

}

```