package logstream

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/minhmannh2001/stream_events_to_kafka_es/internal/pkg/elasticsearch"
	"github.com/minhmannh2001/stream_events_to_kafka_es/internal/pkg/kafka"
)

type publisher struct {
	kafkaProducer *kafka.Producer
	esClient      *elasticsearch.Client

	bufPool       chan []LogEntry
	buf           []LogEntry
	bufSize       int
	maxBufSize    int
	bufLock       sync.Mutex
	sendQueue     chan []LogEntry
	retryInterval time.Duration
	retryNumbers  int

	shutdown     chan struct{}
	shutdownOnce sync.Once
	shutdownWg   sync.WaitGroup
}

func (p *publisher) checkBuf(lastLen int) {
	if len(p.buf) > p.maxBufSize {
		p.flushBuf(lastLen)
	}
}

// flushBuf sends buffer to the queue and initializes new buffer
func (p *publisher) flushBuf(length int) {
	sendBuf := p.buf[0:length]
	tail := p.buf[length:len(p.buf)]

	// get new buffer
	select {
	case p.buf = <-p.bufPool:
		p.buf = p.buf[0:0]
	default:
		p.buf = make([]LogEntry, 0, p.bufSize)
	}

	// copy tail to the new buffer
	p.buf = append(p.buf, tail...)

	// flush current buffer
	p.sendQueue <- sendBuf
}

// flushLoop makes sure logs are flushed every flushInterval
func (p *publisher) flushLoop(flushInterval time.Duration) {
	var flushC <-chan time.Time

	if flushInterval > 0 {
		flushTicker := time.NewTicker(flushInterval)
		defer flushTicker.Stop()
		flushC = flushTicker.C
	}

	for {
		select {
		case <-p.shutdown:
			p.bufLock.Lock()
			if len(p.buf) > 0 {
				p.flushBuf(len(p.buf))
			}
			p.bufLock.Unlock()

			close(p.sendQueue)
			return
		case <-flushC:
			p.bufLock.Lock()
			if len(p.buf) > 0 {
				p.flushBuf(len(p.buf))
			}
			p.bufLock.Unlock()
		}
	}
}

// sendLoop handles log delivery to both kafka and elasticsearch
func (p *publisher) sendLoop(log SomeLogger) {
	var err error

	defer p.shutdownWg.Done()

	for buf := range p.sendQueue {
		if len(buf) > 0 {
			err = SendLogEntriesToKafka(p.kafkaProducer, "logs", convertToInterfaceSlice(buf))
			if err != nil {
				log.Printf("%s Error sending to kafka: %s", DefaultLogPrefix, err)
			}
			err = p.esClient.BulkIndexDocumentsWithRetry("logs", convertToInterfaceSlice(buf), p.retryNumbers, p.retryInterval)
			if err != nil {
				log.Printf("%s Error indexing to es: %s", DefaultLogPrefix, err)
			}
		}

		// return buffer to the pool
		select {
		case p.bufPool <- buf:
		default:
			// pool is full, let GC handle the buf
		}
	}

	p.kafkaProducer.Close() // nolint: gosec
}

func (p *publisher) close() {
	p.shutdownOnce.Do(func() {
		close(p.shutdown)
	})
	p.shutdownWg.Wait()
}

func convertToInterfaceSlice(logEntries []LogEntry) []interface{} {
	var result []interface{}
	for _, logEntry := range logEntries {
		result = append(result, logEntry)
	}
	return result
}

func SendLogEntriesToKafka(p *kafka.Producer, topic string, events []interface{}) error {
	for _, event := range events {
		// Marshal the LogEntry to JSON
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal LogEntry: %w", err)
		}

		// Send the JSON-encoded data as the message value
		err = p.ProduceAsync(topic, nil, data)
		if err != nil {
			return fmt.Errorf("failed to produce message: %w", err)
		}
	}
	return nil
}
