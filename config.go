package kafka

import (
	"fmt"
	"strings"

	config "gitlab.qkids.com/group-api-common/go-conf.git"
)

type KafkaConfig struct {
	Topics           []string `json:"topics"`
	GroupId          string   `json:"group.id"`
	BootstrapServers string   `json:"bootstrap.servers"`
	SecurityProtocol string   `json:"security.protocol"`
	SaslCert         string   `json:"sasl.cert"`
	SaslMechanism    string   `json:"sasl.mechanism"`
	SaslUsername     string   `json:"sasl.username"`
	SaslPassword     string   `json:"sasl.password"`
}

func LoadConfig(prefix string) *KafkaConfig {
	cfgPrefix := "kafka"
	if len(prefix) > 0 {
		cfgPrefix = fmt.Sprintf("kafka.%s", prefix)
	}
	return &KafkaConfig{
		Topics:           strings.Split(config.GetString(fmt.Sprintf("%s.topics", cfgPrefix)), ","),
		BootstrapServers: config.GetString(fmt.Sprintf("%s.servers", cfgPrefix)),
		GroupId:          config.GetString(fmt.Sprintf("%s.consumer", cfgPrefix)),
		SecurityProtocol: config.GetString(fmt.Sprintf("%s.protocal", cfgPrefix)),
		SaslMechanism:    config.GetString(fmt.Sprintf("%s.sasl.mechanism", cfgPrefix)),
		SaslUsername:     config.GetString(fmt.Sprintf("%s.sasl.username", cfgPrefix)),
		SaslPassword:     config.GetString(fmt.Sprintf("%s.sasl.password", cfgPrefix)),
		SaslCert:         config.GetString(fmt.Sprintf("%s.sasl.cert", cfgPrefix)),
	}
}
