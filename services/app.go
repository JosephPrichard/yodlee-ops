package svc

import (
	"os"
	"strings"
	"yodleeops/infra"
)

type App struct {
	*infra.AwsClient
	*infra.KafkaClient
	Broadcaster
}

func MakeApp() *App {
	envMap := make(map[string]string)
	for _, env := range os.Environ() {
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			continue
		}
		envMap[kv[0]] = kv[1]
	}

	config := infra.Config{
		AwsEndpoint:      envMap["AWS_ENDPOINT"],
		AwsDefaultRegion: envMap["AWS_DEFAULT_REGION"],
		KafkaBrokers:     strings.Split(envMap["KAFKA_BROKERS"], ","),
	}

	return &App{
		AwsClient:   infra.MakeAwsClient(config),
		KafkaClient: infra.MakeKafkaConsumerProducer(config),
	}
}
