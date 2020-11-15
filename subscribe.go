package hermes

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/Yamiyo/common/log"
)

type Message struct {
	Topic     string
	GroupID   string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []kafka.Header
	Time      time.Time
}

type ExecuteFunc func(message Message) error

func subscribe(brokers []string, topicName string, groupID string, execute ExecuteFunc, poolSize int) {
	for i := 0; i < poolSize; i++ {
		go goSubscribe(brokers, topicName, groupID, execute)
	}
}

func goSubscribe(brokers []string, topicName string, groupID string, execute ExecuteFunc) {
	for {
		consumption(brokers, topicName, groupID, execute)
	}
}

func consumption(brokers []string, topicName string, groupID string, execute ExecuteFunc) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topicName,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})
	defer reader.Close()

	ctx := context.Background()
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		hermesMsg := Message{
			Topic:     msg.Topic,
			GroupID:   groupID,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   msg.Headers,
			Time:      msg.Time,
		}
		if err := execute(hermesMsg); err != nil {
			log.Error(context.Background(), err)
			time.Sleep(time.Second)
			break
		}
		reader.CommitMessages(ctx, msg)
	}
}
