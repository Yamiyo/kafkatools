package hermes

import (
	"sync"

	"github.com/Shopify/sarama"
)

type KafkaClient interface {
	Subscribe(topicName string, groupID string, execute ExecuteFunc, poolSize int)
	InstanceWriter(topicName string) *kafkaWriter
	InstanceTopicFactory() TopicFactory
}

type kafkaClient struct {
	brokers      []string
	clusterAdmin sarama.ClusterAdmin
	mutex        sync.Mutex
}

func InstanceKafkaClient(brokers []string) KafkaClient {
	return &kafkaClient{
		brokers: brokers,
	}
}

func (k *kafkaClient) Subscribe(topicName string, groupID string, execute ExecuteFunc, poolSize int) {
	subscribe(k.brokers, topicName, groupID, execute, poolSize)
}

func (k *kafkaClient) InstanceWriter(topicName string) *kafkaWriter {
	return instanceWriter(k.brokers, topicName)
}
func (k *kafkaClient) InstanceTopicFactory() TopicFactory {
	return instanceTopicFactory(k.brokers)
}
