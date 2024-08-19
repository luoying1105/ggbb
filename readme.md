### 使用说明文档

#### 项目背景

基于go.etcd.io/bbolt 实现消息订阅模式

#### 安装与依赖

1. 克隆项目：
   ```bash
   git clone http://gitlab.cnns/luoying/gbbolt.git
   ```

2. 安装依赖：
   在项目目录下，执行以下命令安装依赖：
   ```bash
   go mod tidy
   ```

#### 使用说明

假设推送数据结构如下

   ```go
      import gbblot "gitlab.cnns/luoying/gbbolt/pkg"
type testStruct struct {
sid     string
Message string
Time    time.Time
}

   ``` 

1. **消息推送**
   新建队列并且推送结构体
   ```go
   queue, err := gbblot.NewQueue[testStruct]("queue1", &gbblot.JsonCoder[testStruct]{})
   if err != nil {
       fmt.Printf("Error creating queue: %v", err)
   }

   msg := testStruct{
       sid:     "1",
       Message: "Message content",
       Time:    time.Now(),
   }
   err = queue.Enqueue(msg)
   if err != nil {
       fmt.Printf("Error enqueuing message: %v", err)
   }
   ```

2. **消息获取**

   ```go
   consumerID := "consumer1"
   msg, err := queue.Dequeue(consumerID)
   if err != nil {
       fmt.Println("Error dequeuing message:", err)
   } else {
       fmt.Println("Received message:", msg.Data())
   }
   ```

3. **确认消息（ACK）**

   ```go
   err = msg.Ack()
   if err != nil {
       fmt.Println("Error acknowledging message:", err)
   }
   ```

4. **拒绝消息（NACK）**

   ```go
   err = msg.NAck()
   if err != nil {
       fmt.Println("Error rejecting message:", err)
   }
   ```

5. **关闭数据库**

   在程序结束时，关闭数据库连接：
   ```go
   err := gbblot.Close()
   if err != nil {
       fmt.Println("Error closing database:", err)
   }
   ```

6. **清理数据库**

   清理所有已消费的数据：
   ```go
   err := gbblot.CleanDB()
   if err != nil {
       fmt.Println("Error cleaning database:", err)
   }
   ```

#### 示例

```go
package main

import (
	"fmt"
	gbblot "gitlab.cnns/luoying/gbbolt/pkg"

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
	var queues []*gbblot.Queue[testStruct]
	for _, queueName := range queueNames {
		queue, err := gbblot.NewQueue[testStruct](queueName, &gbblot.JsonCoder[testStruct]{})
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
			fmt.Println(fmt.Sprintf("Consumer %s did not receive all messages from %s, received: %d", consumerID, queueNames[i+1], receivedMessages),
			)

		}
	}

	//关闭数据库
	err := gbblot.Close()
	if err != nil {
		fmt.Println(err)
	}

	//清理所有已经推送数据
	err = gbblot.CleanDB()
	if err != nil {
		fmt.Println(err)
	}

}

```
 