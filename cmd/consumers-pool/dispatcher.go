package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

func Dispatcher(
	ctx context.Context,
	producerChan chan interface{},
	consumerChan chan<- interface{},
	mCounter *MetricsCounter,
	producersSync *sync.WaitGroup,
	globalStop *atomic.Bool,
) {
	fmt.Println("Starting dispatcher")

	go func() {
		<-ctx.Done()
		fmt.Println("Stopping dispatcher")
		producersSync.Wait()   // ждем завершения всех продьюсеров, возмо
		close(producerChan)    // закрываем спокойно канал продьюсеров
		globalStop.Store(true) //  сигнализируем консьюмнерам, чтобы заканчивали делать балансировку
	}()

	for task := range producerChan {
		mCounter.receivedDispatcher.Add(1)
		consumerChan <- task
		mCounter.sentDispatcher.Add(1)
	}
	close(consumerChan)
	fmt.Println("Stopped dispatcher")
}
