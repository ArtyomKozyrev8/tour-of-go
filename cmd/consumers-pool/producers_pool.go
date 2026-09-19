package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ProducerPool struct {
	num              int             // Число продьюсеров в пуле
	taskCounter      atomic.Int64    // создаем задачи
	stopCtx          context.Context // глобальный контекст из приложения
	syncGroup        *sync.WaitGroup // синхронизация с внешним кодом
	producersChannel chan interface{}
	mCounter         *MetricsCounter
}

func NewProducerPool(num int, stopCtx context.Context, syncGroup *sync.WaitGroup, mCounter *MetricsCounter) *ProducerPool {
	return &ProducerPool{
		num:              num,
		stopCtx:          stopCtx,
		producersChannel: make(chan interface{}, 1000),
		syncGroup:        syncGroup,
		mCounter:         mCounter,
	}
}

func (pp *ProducerPool) createProducer(name string) {
	fmt.Printf("%s was created\n", name)
	timer := time.NewTimer(time.Millisecond * time.Duration(100+rand.Intn(500)))

	for {
		select {
		case <-timer.C:
			cancelContext, cancel := context.WithTimeout(pp.stopCtx, time.Millisecond*100)
			task := pp.taskCounter.Add(1)
			pp.mCounter.createdProducer.Add(1)
			select {
			case <-cancelContext.Done():
				pp.mCounter.droppedProducer.Add(1)
			case pp.producersChannel <- task:
				pp.mCounter.sentProducer.Add(1)
			}
			cancel()
			timer.Reset(time.Millisecond * time.Duration(100+rand.Intn(500)))
		case <-pp.stopCtx.Done():
			fmt.Printf("%s is stopped\n", name)
			return
		}
	}
}

func (pp *ProducerPool) Start() {
	for i := 0; i < pp.num; i++ {
		name := fmt.Sprintf("%d", i)
		pp.syncGroup.Go(func() {
			pp.createProducer(name)
		})
	}
}
