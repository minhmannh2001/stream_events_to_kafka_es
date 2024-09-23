package logstream

import (
	"time"

	"github.com/minhmannh2001/stream_events_to_kafka_es/internal/pkg/elasticsearch"
	"github.com/minhmannh2001/stream_events_to_kafka_es/internal/pkg/kafka"
)

const (
	DefaultMaxBufSize        = 256
	DefaultFlushInterval     = 100 * time.Millisecond
	DefaultRetryInterval     = 3 * time.Second
	DefaultRetryNumbers      = 5
	DefaultBufPoolCapacity   = 20
	DefaultSendQueueCapacity = 10
	DefaultSendLoopCount     = 2
	DefaultLogPrefix         = "[logstream]"
)

type SomeLogger interface {
	Printf(fmt string, args ...interface{})
	Fatal(args ...interface{})
}

type LogStreamOptions struct {
	KafkaProducer     *kafka.Producer
	EsClient          *elasticsearch.Client
	FlushInterval     time.Duration
	RetryInterval     time.Duration
	RetryNumbers      int
	MaxBufSize        int
	BufPoolCapacity   int
	SendQueueCapacity int
	SendLoopCount     int
	Logger            SomeLogger
}

type Option func(c *LogStreamOptions)

func KafkaProducer(producer *kafka.Producer) Option {
	return func(c *LogStreamOptions) {
		c.KafkaProducer = producer
	}
}

func ElasticsearchClient(client *elasticsearch.Client) Option {
	return func(c *LogStreamOptions) {
		c.EsClient = client
	}
}

func FlushInterval(interval time.Duration) Option {
	return func(c *LogStreamOptions) {
		c.FlushInterval = interval
	}
}

func RetryInterval(interval time.Duration) Option {
	return func(c *LogStreamOptions) {
		c.RetryInterval = interval
	}
}

func RetryNumbers(numbers int) Option {
	return func(c *LogStreamOptions) {
		c.RetryNumbers = numbers
	}
}

func BufPoolCapacity(capacity int) Option {
	return func(c *LogStreamOptions) {
		c.BufPoolCapacity = capacity
	}
}

func MaxBufSize(size int) Option {
	return func(c *LogStreamOptions) {
		c.MaxBufSize = size
	}
}

func SendQueueCapacity(capacity int) Option {
	return func(c *LogStreamOptions) {
		c.SendQueueCapacity = capacity
	}
}

func SendLoopCount(count int) Option {
	return func(c *LogStreamOptions) {
		c.SendLoopCount = count
	}
}

func Logger(logger SomeLogger) Option {
	return func(c *LogStreamOptions) {
		c.Logger = logger
	}
}
