package main

import (
	"context"
	"log"
	"time"

	"producer/internal/config"
	"producer/internal/kafkaclient"
	"producer/internal/loader"
)

func main() {
	cfg := config.Load()

	records, err := loader.LoadJSONL(cfg.DataFile)
	if err != nil {
		log.Fatalf("failed to load data file %s: %v", cfg.DataFile, err)
	}
	log.Printf("loaded %d records from %s", len(records), cfg.DataFile)

	producer := kafkaclient.New(cfg.KafkaBrokers, cfg.TopicName)
	defer producer.Close()

	ctx := context.Background()
	delay := time.Duration(cfg.DelayMs) * time.Millisecond

	for {
		for i, rec := range records {
			if err := producer.Publish(ctx, rec); err != nil {
				log.Printf("failed to publish record %s: %v", rec.ID, err)
				continue
			}
			if (i+1)%100 == 0 {
				log.Printf("published %d/%d records", i+1, len(records))
			}
			time.Sleep(delay)
		}
		log.Printf("finished streaming %d records", len(records))
		if !cfg.Loop {
			break
		}
		log.Println("looping again (PRODUCER_LOOP=true)")
	}
}
