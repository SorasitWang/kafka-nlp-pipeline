package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers []string
	TopicName    string
	DataFile     string
	DelayMs      int
	Loop         bool
}

func Load() Config {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	delayMs, err := strconv.Atoi(getEnv("PRODUCER_DELAY_MS", "200"))
	if err != nil {
		delayMs = 200
	}
	loop := strings.ToLower(getEnv("PRODUCER_LOOP", "false")) == "true"

	return Config{
		KafkaBrokers: strings.Split(brokers, ","),
		TopicName:    getEnv("TOPIC_NAME", "text-stream"),
		DataFile:     getEnv("PRODUCER_DATA_FILE", "../data/dataset.jsonl"),
		DelayMs:      delayMs,
		Loop:         loop,
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
