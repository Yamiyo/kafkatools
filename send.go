package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type KafkaWriter interface {
	Send(writerMsg ...interface{}) error
	SendRetry(writerMsg ...interface{}) error
	SendWithHash(writerMsg ...*WriterMessage) error
	SendWithHashRetry(writerMsg ...*WriterMessage) error
	SendWithBatch(size int, objs interface{}) error
	SendWithBatchRetry(size int, objs interface{}) error
}

type kafkaWriter struct {
	writer *kafka.Writer
}

type WriterMessage struct {
	Key   string
	Value interface{}
}

func instanceWriter(brokers []string, topicName string) *kafkaWriter {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topicName,
		BatchTimeout: 100 * time.Millisecond,
	})

	writer.Balancer = &kafka.Hash{}

	return &kafkaWriter{
		writer,
	}
}

func (k *kafkaWriter) Send(writerMsg ...interface{}) error {
	if len(writerMsg) == 0 {
		return nil
	}
	WriterMessages, err := messageConvert(writerMsg...)
	if err != nil {
		return err
	}

	return k.send(false, WriterMessages...)
}

func (k *kafkaWriter) SendRetry(writerMsg ...interface{}) error {
	if len(writerMsg) == 0 {
		return nil
	}
	WriterMessages, err := messageConvert(writerMsg...)
	if err != nil {
		return err
	}
	return k.send(true, WriterMessages...)
}

func (k *kafkaWriter) SendWithHash(writerMsg ...*WriterMessage) error {
	if len(writerMsg) == 0 {
		return nil
	}
	WriterMessages, err := writerMessageConvert(writerMsg...)
	if err != nil {
		return err
	}
	return k.send(false, WriterMessages...)
}

func (k *kafkaWriter) SendWithHashRetry(writerMsg ...*WriterMessage) error {
	if len(writerMsg) == 0 {
		return nil
	}
	WriterMessages, err := writerMessageConvert(writerMsg...)
	if err != nil {
		return err
	}
	return k.send(true, WriterMessages...)
}

func (k *kafkaWriter) SendWithBatch(size int, objs interface{}) error {
	objInterface, err := sliceConvert(objs)
	if err != nil {
		return err
	}
	if len(objInterface) == 0 {
		return nil
	}
	WriterMessages, err := batchMessageConvert(size, objInterface...)
	if err != nil {
		return err
	}
	return k.send(false, WriterMessages...)
}

func (k *kafkaWriter) SendWithBatchRetry(size int, objs interface{}) error {
	objInterface, err := sliceConvert(objs)
	if err != nil {
		return err
	}
	if len(objInterface) == 0 {
		return nil
	}
	WriterMessages, err := batchMessageConvert(size, objInterface...)
	if err != nil {
		return err
	}
	return k.send(true, WriterMessages...)
}

func (k *kafkaWriter) send(retry bool, writerMsg ...kafka.Message) error {
	for {
		for _, msg := range writerMsg {
			if msg.Key == nil {
				msg.Key = []byte(uuid.New().String())
			}
		}
		if err := k.writer.WriteMessages(context.Background(), writerMsg...); !retry {
			return err
		} else if err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
}

func messageConvert(writerMsg ...interface{}) ([]kafka.Message, error) {
	var WriterMessages []kafka.Message
	for _, msg := range writerMsg {
		value, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		WriterMessages = append(WriterMessages, kafka.Message{
			Key:   nil,
			Value: value,
		})
	}

	return WriterMessages, nil
}

func writerMessageConvert(writerMsg ...*WriterMessage) ([]kafka.Message, error) {
	var WriterMessages []kafka.Message
	for _, msg := range writerMsg {
		value, err := json.Marshal(msg.Value)
		if err != nil {
			return nil, err
		}
		WriterMessages = append(WriterMessages, kafka.Message{
			Key:   []byte(msg.Key),
			Value: value,
		})
	}

	return WriterMessages, nil
}

func batchMessageConvert(size int, objs ...interface{}) ([]kafka.Message, error) {
	var WriterMessages []kafka.Message
	var objBatch []interface{}
	count := 0
	for _, msg := range objs {
		count++
		objBatch = append(objBatch, msg)
		if count >= size {
			kafkaMessage, err := messageConvert(objBatch)
			if err != nil {
				return nil, err
			}
			WriterMessages = append(WriterMessages, kafkaMessage...)
			objBatch = []interface{}{}
			count = 0
		}
	}
	kafkaMessage, err := messageConvert(objBatch)
	if err != nil {
		return nil, err
	}
	WriterMessages = append(WriterMessages, kafkaMessage...)

	return WriterMessages, nil
}

func sliceConvert(obj interface{}) ([]interface{}, error) {
	var objInterfaces []interface{}
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Slice:
		slice := reflect.ValueOf(obj)
		for i := 0; i < slice.Len(); i++ {
			objInterfaces = append(objInterfaces, slice.Index(i).Interface())
		}
	default:
		return nil, errors.New("SendSizeWithKeyRetry convert to slice error")
	}
	return objInterfaces, nil
}
