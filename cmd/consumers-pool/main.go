package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const ProducersNum = 8
const ProgramDuration = 180

const MinWorkersNum = 2
const MaxWorkersNum = 32

func main() {
	timeoutContext, timeoutCancel := context.WithTimeout(context.Background(), ProgramDuration*time.Second)
	metricsCounter := &MetricsCounter{}
	syncGroupProducers := &sync.WaitGroup{}
	globalStop := &atomic.Bool{}
	defer timeoutCancel()
	syncGroupMain := &sync.WaitGroup{}

	go func() {
		ticker := time.NewTicker(time.Second)

		for {
			<-ticker.C
			fmt.Println(metricsCounter)
		}
	}()

	producersPool := NewProducerPool(ProducersNum, timeoutContext, syncGroupProducers, metricsCounter)
	go func() { producersPool.Start() }()

	workersPool := NewWorkersPool(MinWorkersNum, MaxWorkersNum, globalStop, metricsCounter, syncGroupMain)
	go func() { workersPool.Start() }()

	syncGroupMain.Go(func() {
		Dispatcher(timeoutContext, producersPool.producersChannel, workersPool.workersChan, metricsCounter, syncGroupProducers, globalStop)
	})
	syncGroupMain.Wait()
	fmt.Println("ALL DONE")
	fmt.Println(metricsCounter)
}
