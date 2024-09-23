package main

import (
	"fmt"
	"log"
	"time"

	"github.com/minhmannh2001/stream_events_to_kafka_es/internal/logstream"
	"github.com/minhmannh2001/stream_events_to_kafka_es/internal/pkg/elasticsearch"
	"github.com/minhmannh2001/stream_events_to_kafka_es/internal/pkg/kafka"
)

func main() {
	kafkaProducer, err := kafka.NewProducer("localhost:29092")
	if err != nil {
		log.Fatal(err)
	}

	esClient, err := elasticsearch.NewClient([]string{"http://localhost:9200"})
	if err != nil {
		log.Fatal(err)
	}

	ls := logstream.NewLogStream(
		logstream.KafkaProducer(kafkaProducer),
		logstream.ElasticsearchClient(esClient),
	)

	numEntries := 5
	for i := 0; i < numEntries; i++ {
		// Generate a random LogEntry object
		entry := logstream.LogEntry{
			ID:        fmt.Sprintf("log-%d", i+1),
			Timestamp: time.Now(),
			Message:   fmt.Sprintf("Generated message #%d", i+1),
		}

		time.Sleep(time.Second * 3)
		// Add the entry to the LogStream
		ls.Add(entry)
	}

	for {
		// This loop will run indefinitely
	}
}
