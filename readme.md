基于之前的代码调整和需求，我将为你提供一个更新后的 README 文件示例。这个 README 将涵盖项目的概要、安装步骤、使用方法、代码示例，以及注意事项。

### 更新后的 README

```markdown
# Bunnymq 使用指南

## 1. 项目简介

`Bunnymq` 是一个基于 BoltDB 的高效消息队列库，支持消息的生产、消费、确认（ACK）、拒绝（NACK），以及对已消费消息的清理功能。它通过在单个数据库文件中使用多个 `bucket`，实现了多个队列的独立管理。

## 2. 安装

通过以下命令安装 `Bunnymq`：

```bash
go env -w GOPRIVATE=gitlab.cnns
go get gitlab.cnns/luoying/bunnymq@v0.0.2
```

## 3. 使用方法

### 3.1 创建队列并推送消息

在创建队列时，每个队列使用独立的 `bucket` 存储消息，并且可以共享或独立使用数据库文件。

```go
package main

import (
    "fmt"
    "time"
    "gitlab.cnns/luoying/bunnymq/pkg"
)

type testStruct struct {
    sid     string
    Message string
    Time    time.Time
}

func main() {
    dbPath := "test.db"
    queueNames := []string{"queue1", "queue2"}

    for _, queueName := range queueNames {
        queue, err := bunnymq.NewQueue[testStruct](queueName, dbPath, &bunnymq.JsonCoder[testStruct]{})
        if err != nil {
            fmt.Printf("Error creating queue %s: %v", queueName, err)
            continue
        }

        // 推送消息到队列
        for i := 1; i <= 30; i++ {
            msg := testStruct{
                sid:     queueName,
                Message: fmt.Sprintf("Message %d", i),
                Time:    time.Now(),
            }
            if err := queue.Enqueue(msg); err != nil {
                fmt.Printf("Error enqueuing message to %s: %v", queueName, err)
            }
        }

        // 关闭队列
        if err := queue.Close(); err != nil {
            fmt.Printf("Error closing queue %s: %v", queueName, err)
        }
    }
}
```

### 3.2 消费消息并确认（ACK）

使用 `Dequeue` 方法消费消息，并在成功处理后调用 `Ack` 方法确认消息。

```go
consumerID := "consumer1"
queue, err := bunnymq.NewQueue[testStruct]("queue1", "test.db", &bunnymq.JsonCoder[testStruct]{})
if err != nil {
    fmt.Printf("Error creating queue: %v", err)
    return
}
defer queue.Close()

for {
    msg, err := queue.Dequeue(consumerID)
    if err != nil {
        break
    }

    fmt.Printf("Received message: %s\n", msg.Data().Message)
    if err := msg.Ack(); err != nil {
        fmt.Printf("Error acknowledging message: %v", err)
    }
}
```

### 3.3 消息拒绝（NACK）

如果消息处理失败，可以使用 `NAck` 方法拒绝消息。

```go
msg, err := queue.Dequeue(consumerID)
if err != nil {
    fmt.Printf("Error dequeuing message: %v", err)
    return
}

if err := msg.NAck(); err != nil {
    fmt.Printf("Error rejecting message: %v", err)
}
```

### 3.4 清理已消费的消息

使用 `CleanDB` 方法清理已经确认的消息。

```go
err := bunnymq.CleanDB()
if err != nil {
    fmt.Println("Error cleaning database:", err)
}
```

### 3.5 关闭数据库连接

在程序结束时，请确保关闭所有打开的数据库连接。

```go
err := bunnymq.Close()
if err != nil {
    fmt.Println("Error closing database:", err)
}
```

## 4. 注意事项

- **独立消费者进度管理**：确保每个消费者使用唯一的 `consumerID` 来管理自己的消费进度。
- **共享或独立的数据库文件**：同一个数据库文件可以在多个队列间共享，也可以独立使用 
- **程序结束时关闭连接**：确保在程序结束时，所有数据库连接都被正确关闭，以防止资源泄漏。

## 5. 示例项目

以下是一个完整的示例项目，展示了如何使用 `Bunnymq` 创建多个队列、推送消息、消费消息，以及进行数据库清理：

```go
package main

import (
    "fmt"
    "time"
    "gitlab.cnns/luoying/bunnymq/pkg"
)

type testStruct struct {
    sid     string
    Message string
    Time    time.Time
}

func main() {
    dbPath := "example.db"
    queueNames := []string{"queue1", "queue2"}

    var queues []*bunnymq.Queue[testStruct]
    for _, queueName := range queueNames {
        queue, err := bunnymq.NewQueue[testStruct](queueName, dbPath, &bunnymq.JsonCoder[testStruct]{})
        if err != nil {
            fmt.Printf("Error creating queue %s: %v", queueName, err)
            continue
        }
        queues = append(queues, queue)
    }

    // 推送消息到队列
    for i, queue := range queues {
        for j := 1; j <= 30; j++ {
            msg := testStruct{
                sid:     fmt.Sprintf("Queue%d", i+1),
                Message: fmt.Sprintf("Message %d", j),
                Time:    time.Now(),
            }
            if err := queue.Enqueue(msg); err != nil {
                fmt.Printf("Error enqueuing message to %s: %v", queueNames[i], err)
            }
        }
    }

    // 消费并确认消息
    consumerID := "consumer1"
    for _, queue := range queues {
        for {
            msg, err := queue.Dequeue(consumerID)
            if err != nil {
                break
            }
            fmt.Printf("Received message from %s: %s\n", queue.queueName, msg.Data().Message)
            if err := msg.Ack(); err != nil {
                fmt.Printf("Error acknowledging message: %v", err)
            }
        }
    }

    // 清理数据库
    err := bunnymq.CleanDB()
    if err != nil {
        fmt.Println("Error cleaning database:", err)
    }

    // 关闭所有队列
    for _, queue := range queues {
        if err := queue.Close(); err != nil {
            fmt.Printf("Error closing queue %s: %v", queue.queueName, err)
        }
    }
}
```
 