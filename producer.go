package kafka

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/go-uuid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var cfg *KafkaConfig
var producers = make(map[string]*kafka.Producer)

const (
	INT32_MAX = 2147483647 - 1000
)

func getProducer(cfgPrefix string) *kafka.Producer {
	key := cfgPrefix
	if len(key) == 0 {
		key = "_MAIN_"
	}
	cfg = LoadConfig(cfgPrefix)
	if producer, ok := producers[key]; ok {
		return producer
	}
	var err error
	var kafkaconf = &kafka.ConfigMap{
		"api.version.request":           "true",
		"message.max.bytes":             1000000,
		"linger.ms":                     500,
		"sticky.partitioning.linger.ms": 1000,
		"retries":                       INT32_MAX,
		"retry.backoff.ms":              1000,
		"acks":                          "1"}
	kafkaconf.SetKey("bootstrap.servers", cfg.BootstrapServers)

	switch cfg.SecurityProtocol {
	case "PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "plaintext")
	case "SASL_SSL":
		kafkaconf.SetKey("security.protocol", "sasl_ssl")
		kafkaconf.SetKey("ssl.ca.location", "conf/ca-cert.pem")
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername)
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword)
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism)
	case "SASL_PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "sasl_plaintext")
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername)
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword)
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism)
	default:
		panic(kafka.NewError(kafka.ErrUnknownProtocol, "unknown protocol", true))
	}

	producer, err := kafka.NewProducer(kafkaconf)
	if err != nil {
		panic(err)
	}
	producers[key] = producer
	fmt.Print("init kafka producer success\n")
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Failed to write access log entry:%v", ev.TopicPartition.Error)
				} else {
					log.Printf("Send OK topic:%v partition:%v offset:%v content:%s\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, ev.Value)

				}
			}
		}
	}()
	return producer
}

type ProduceRes struct {
	Topic     string
	Partition int32
	Offset    string
	Err       error
}

func Produce(cfgPrefix, content string) (string, []ProduceRes, error) {
	producer := getProducer(cfgPrefix)
	cfg = LoadConfig(cfgPrefix)
	var res []ProduceRes
	key, err := uuid.GenerateUUID()
	if err != nil {
		return "", res, err
	}
	for _, topic := range cfg.Topics {
		deliveryChan := make(chan kafka.Event)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(content),
			Timestamp:      time.Now(),
		}
		err := producer.Produce(msg, deliveryChan)

		e := <-deliveryChan
		m := e.(*kafka.Message)
		var topicRes ProduceRes
		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			topicRes = ProduceRes{
				Topic: topic,
				Err:   err,
			}
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			topicRes = ProduceRes{
				Topic:     topic,
				Partition: m.TopicPartition.Partition,
				Offset:    m.TopicPartition.Offset.String(),
				Err:       err,
			}
		}
		res = append(res, topicRes)
	}
	producer.Flush(15 * 1000)
	return key, res, err
}
