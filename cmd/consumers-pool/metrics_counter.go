package main

import (
	"fmt"
	"sync/atomic"
)

type MetricsCounter struct {
	createdProducer    atomic.Int64 // Произведено задач продьюсерами
	sentProducer       atomic.Int64 // Отправлено задач продьюсерами
	droppedProducer    atomic.Int64 // Отброшено задач продьюсерами
	receivedDispatcher atomic.Int64 // Получено задач диспетчером
	droppedDispatcher  atomic.Int64 // Отброшено задач диспетчером
	sentDispatcher     atomic.Int64 // Отправлено задач диспетчером
	receivedConsumer   atomic.Int64 // Получено задач консьюмерами
	droppedConsumer    atomic.Int64 //  Отброшено задач консьюмерами
	processedConsumer  atomic.Int64 // Обработано задач консьюмерами
}

func (m *MetricsCounter) String() string {
	return fmt.Sprintf(
		"MetricsCounter{createdProducer=%d, sentProducer=%d, droppedProducer=%d, receivedDispatcher=%d, droppedDispatcher=%d, sentDispatcher=%d, receivedConsumer=%d, droppedConsumer=%d, processedConsumer=%d}",
		m.createdProducer.Load(),
		m.sentProducer.Load(),
		m.droppedProducer.Load(),
		m.receivedDispatcher.Load(),
		m.droppedDispatcher.Load(),
		m.sentDispatcher.Load(),
		m.receivedConsumer.Load(),
		m.droppedConsumer.Load(),
		m.processedConsumer.Load(),
	)
}
