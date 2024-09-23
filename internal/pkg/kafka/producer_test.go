package kafka

import (
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockKafkaProducer struct {
	mock.Mock
}

func (m *MockKafkaProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := m.Called(msg, deliveryChan)
	return args.Error(0)
}

func (m *MockKafkaProducer) Events() chan kafka.Event {
	deliveryChan := make(chan kafka.Event)
	go func() {
		topic := "test-topic"
		deliveryReport := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: 1,
				Offset:    100,
			},
			Value: []byte("test-value"),
			Key:   []byte("test-key"),
		}
		deliveryChan <- deliveryReport
	}()
	return deliveryChan
}

func (m *MockKafkaProducer) Close() {
	m.Called()
}

func TestProduceAsync(t *testing.T) {
	mockProducer := new(MockKafkaProducer)
	producer := &Producer{producer: mockProducer}

	topic := "test-topic"
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Successful production", func(t *testing.T) {
		mockProducer.On("Produce", mock.Anything, mock.Anything).Return(nil).Once()

		err := producer.ProduceAsync(topic, key, value)
		assert.NoError(t, err)

		mockProducer.AssertExpectations(t)
	})

	t.Run("Production failure", func(t *testing.T) {
		expectedErr := errors.New("production failed")
		mockProducer.On("Produce", mock.Anything, mock.Anything).Return(expectedErr).Once()

		err := producer.ProduceAsync(topic, key, value)
		assert.Error(t, err)
		assert.Equal(t, "failed to produce message: production failed", err.Error())

		mockProducer.AssertExpectations(t)
	})
}

func TestClose(t *testing.T) {
	mockProducer := new(MockKafkaProducer)
	producer := &Producer{producer: mockProducer}

	mockProducer.On("Close").Return().Once()

	producer.Close()

	mockProducer.AssertExpectations(t)
}
