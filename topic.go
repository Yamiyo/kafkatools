package hermes

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Yamiyo/common/log"
)

type TopicFactory interface {
	CreateTopic(topicName string, partitions int)
	Close()
}

type topicFactory struct {
	brokers      []string
	clusterAdmin sarama.ClusterAdmin
}

func instanceTopicFactory(brokers []string) *topicFactory {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // To enable consumer group, but will cause disable of 'auto.create.topic'
	client, err := sarama.NewClusterAdmin(brokers, config)
	for err != nil {
		log.Error(context.Background(), err.Error())
		client, err = sarama.NewClusterAdmin(brokers, config)
		time.Sleep(time.Second)
	}
	return &topicFactory{
		brokers:      brokers,
		clusterAdmin: client,
	}
}

func (t *topicFactory) CreateTopic(topicName string, partitions int) {
	partitionsInt32 := int32(partitions)
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitionsInt32,
		ReplicationFactor: int16(len(t.brokers)),
		ConfigEntries:     make(map[string]*string),
	}
	t.clusterAdmin.CreateTopic(topicName, topicDetail, false)
	if err := t.clusterAdmin.CreateTopic(topicName, topicDetail, false); err != nil {
		t.clusterAdmin.CreatePartitions(topicName, partitionsInt32, nil, false)
	}
}

func (t *topicFactory) Close() {
	t.clusterAdmin.Close()
}
