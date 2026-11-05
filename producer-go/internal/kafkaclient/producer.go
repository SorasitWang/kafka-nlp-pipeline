package kafkaclient

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"producer/internal/loader"
)

type Message struct {
	ID            string `json:"id"`
	Text          string `json:"text"`
	SourceDataset string `json:"source_dataset"`
	ProducedAt    string `json:"produced_at"`
}

type Producer struct {
	writer *kafka.Writer
}

func New(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // key-based partitioning across the topic's partitions
			RequiredAcks: kafka.RequireOne,
			// Default BatchTimeout (1s) makes low-throughput single-message
			// writes wait a full second for the batch to flush; since we're
			// streaming one record at a time with our own inter-message
			// delay, flush near-immediately instead.
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) Publish(ctx context.Context, rec loader.Record) error {
	msg := Message{
		ID:            rec.ID,
		Text:          rec.Text,
		SourceDataset: rec.SourceDataset,
		ProducedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(rec.ID),
		Value: value,
	})
}
