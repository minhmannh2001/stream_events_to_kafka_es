package logstream

import (
	"log"
	"os"
)

type LogStream struct {
	pub *publisher
}

func NewLogStream(options ...Option) *LogStream {
	opts := LogStreamOptions{
		FlushInterval:     DefaultFlushInterval,
		RetryInterval:     DefaultRetryInterval,
		RetryNumbers:      DefaultRetryNumbers,
		MaxBufSize:        DefaultMaxBufSize,
		BufPoolCapacity:   DefaultBufPoolCapacity,
		SendQueueCapacity: DefaultSendQueueCapacity,
		SendLoopCount:     DefaultSendLoopCount,
		Logger:            log.New(os.Stderr, DefaultLogPrefix, log.LstdFlags),
	}

	ls := &LogStream{
		pub: &publisher{
			shutdown: make(chan struct{}),
		},
	}

	ls.pub.bufSize = opts.MaxBufSize + 10

	for _, option := range options {
		option(&opts)
	}

	ls.pub.kafkaProducer = opts.KafkaProducer
	ls.pub.esClient = opts.EsClient
	ls.pub.buf = make([]LogEntry, 0, opts.BufPoolCapacity)
	ls.pub.bufPool = make(chan []LogEntry, opts.BufPoolCapacity)
	ls.pub.sendQueue = make(chan []LogEntry, opts.SendQueueCapacity)
	ls.pub.retryInterval = opts.RetryInterval
	ls.pub.retryNumbers = opts.RetryNumbers

	go ls.pub.flushLoop(opts.FlushInterval)

	for i := 0; i < opts.SendLoopCount; i++ {
		ls.pub.shutdownWg.Add(1)
		go ls.pub.sendLoop(opts.Logger)
	}

	return ls
}

func (c *LogStream) Close() error {
	c.pub.close()
	return nil
}

func (ls *LogStream) Add(logEntry LogEntry) {
	ls.pub.bufLock.Lock()
	ls.pub.buf = append(ls.pub.buf, logEntry)
	ls.pub.checkBuf(len(ls.pub.buf))
	ls.pub.bufLock.Unlock()
}
