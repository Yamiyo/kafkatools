# hermes
kafka 接收與推送信息的工具

## Quick Start
### 生成KafkaClient
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
```
- brokers 設定 kafka 的 host,作為溝通的依據

### 建立Topic
***
#### 單個 Topic
如果用此方法創多個 topic 效能較差
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
topicFactory := kafkaClient.InstanceTopicFactory()
defer topicFactory.Close()
topicName :="test_topic"
partitionSize:= 10
topicFactory.CreateTopic(topicName, partitionSize)
```
- topicName topic 的名稱
- partitionSize 預計的 partition 數量

#### 多個 Topic
因只需要拉一條連線效能較好
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
topicFactory := kafkaClient.InstanceTopicFactory()
defer topicFactory.Close()
topicNames := []string{"test_topic1","test_topic2","test_topic3"}
for topicName := range topicNames {
    partitionSize:= 10
    topicFactory.CreateTopic(topicName, partitionSize)
}
```
- topicName topic 的名稱
- partitionSize 預計的 partition 數量

### 接收信息
***
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
callBackFunc := func(message hermes.Message) error {
	return nil
}
topicName :="test_topic"
groupID :="group_id"
poolSize:= 5
kafkaClient.Subscribe(topicName, groupID, callBackFunc, poolSize)
```
- topicName topic 的名稱
- groupID 使用同個 groupID 可讓服務進行 cluster 的消費模式
- callBackFunc 監聽到時所執行的 function
- poolSize 可以決定多少個執行續執行消費

### 推送信息
***
#### 生成 Writer
建議每個 topic 的 kafkaWriter 做成獨體模式,才能有效發揮推送的效能
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
topicName :="test_topic"
kafkaWriter := kafkaClient.InstanceWriter(topicName)
```
#### 一般推送
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
topicName :="test_topic"
kafkaWriter := kafkaClient.InstanceWriter(topicName)
message1:="message1"
message2:=2
// 一般推送 失敗會重試 10 次
err := kafkaWriter.Send(message1,message2)
// 重試推送 失敗會推到成功為止
kafkaWriter.SendRetry(message1,message2)
```
message 可接收多個 interface{},此方法為 partition 隨機推送

#### Hash推送
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
topicName :="test_topic"
kafkaWriter := kafkaClient.InstanceWriter(topicName)
message1 := &WriterMessage{
    Key: "1",
    Value: "message1",
}
message2 := &WriterMessage{
	Key: "1", 
    Value: 2,
}
// 一般推送 失敗會重試 10 次
err := kafkaWriter.SendWithHash(message1,message2)
// 重試推送 失敗會推到成功為止
kafkaWriter.SendWithHashRetry(message1,message2)
```
message 可接收多個 interface{},此方法為 partition Hash推送
此推送可以保證同樣 Key 的消息有序,上述範例兩筆 Message 會被推到同個 partition 裡

#### Batch推送
```go
brokers := []string{"127.0.0.1:9092","127.0.0.1:9093")
kafkaClient := hermes.InstanceKafkaClient(brokers)
topicName :="test_topic"
kafkaWriter := kafkaClient.InstanceWriter(topicName)
messages := []interface{}{}
messages = append(messages, "message1")
messages = append(messages, "message2")
messages = append(messages, "message3")
for i := 1; i <= 1000; i++ {
	messages = append(messages, i)
}
batchSize := 50
// 一般推送 失敗會重試 10 次
err := kafkaWriter.SendWithBatch(batchSize, messages)
// 重試推送 失敗會推到成功為止
kafkaWriter.SendWithBatchRetry(batchSize, messages)
```
message 可接收多個 interface{},此方法為 partition Batch推送
此推送可以 Slice 消息進行切割推送,上述範例共有1003筆消息,當進行50的切割時會被分成 50,50,....,50,3 的 slice 被推送
- batchSize 切割的大小
- messages 可以為任意型態的 Slice ex:[]string, []int ...