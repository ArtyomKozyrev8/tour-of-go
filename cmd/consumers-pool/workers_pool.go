package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type RateCounter struct {
	rate atomic.Int64
}

type WorkersPool struct {
	minNum     int        // Минимальное кол-во воркеров в пуле
	maxNum     int        // Максимальное кол-во воркеров в пуле
	curNum     int        // Текущее кол-во воркеров в пуле
	scaleMutex sync.Mutex // Синхронизация балансировки

	cancelSlice   []context.CancelFunc // Храним для остановки отдельных воркеров
	workersChan   chan interface{}     // Канал входных задач
	waitGroup     sync.WaitGroup       // Нужно для синхронизации остановки воркеров
	nameCounter   int                  // Счетчик имен воркеров
	rateCounter   *RateCounter         // Внешний источник увеличивает его, пул обнуляем его при ребалансировке
	maxBorderRate int                  // Если выше - увеличиваем кол-во воркеров
	minBorderRate int                  // Если темп ниже - уменьшаем кол-во воркеров
	globalStop    atomic.Bool          // Внешний источник показывает что новых задач не будет (останавливает балансировку)
}

// NewWorkersPool создает новый пул воркеров
func NewWorkersPool(minNum int, maxNum int) *WorkersPool {
	return &WorkersPool{
		minNum: minNum,
		maxNum: maxNum,
	}
}

// Логика работы воркеров
func (wp *WorkersPool) runWorker(name string, stopContext context.Context) {
	fmt.Printf("%s worker starting...\n", name)
	for {
		select {
		case <-stopContext.Done():
			fmt.Printf("%s was stopped (was cancelled)\n", name)
			return
		case task, ok := <-wp.workersChan:
			if !ok {
				fmt.Printf("%s worker stopped. All tasks were done\n", name)
				return
			}
			time.Sleep(time.Duration(300+rand.Intn(700)) * time.Millisecond)
			fmt.Printf("%s did task %v.\n", name, task)
		}
	}
}

// Создает новый воркер
func (wp *WorkersPool) createNewWorker() {
	wp.nameCounter++
	wp.curNum++
	stopContext, cancel := context.WithCancel(context.Background())
	name := fmt.Sprintf("Worker-%d", wp.nameCounter)
	wp.waitGroup.Go(func() {
		wp.runWorker(name, stopContext)
	})
	wp.cancelSlice = append(wp.cancelSlice, cancel)
}

// Отменяет один из воркеров
func (wp *WorkersPool) cancelWorker() {
	wp.cancelSlice[len(wp.cancelSlice)-1]()
	wp.cancelSlice = wp.cancelSlice[:len(wp.cancelSlice)-1]
	wp.curNum--
}

// Балансирует кол-во воркеров в пуле
func (wp *WorkersPool) balanceWorkersNum() {
	wp.scaleMutex.Lock()
	defer wp.scaleMutex.Unlock()

	rate := int(wp.rateCounter.rate.Load())

	if wp.curNum < wp.maxNum && rate > wp.maxBorderRate {
		wp.rateCounter.rate.Store(0)
		wp.createNewWorker()
	} else if wp.curNum > wp.minNum && rate < wp.minBorderRate {
		wp.rateCounter.rate.Store(0)
		wp.cancelWorker()
	} else {
		wp.rateCounter.rate.Store(0)
	}
}

// Start запускает работу пула
func (wp *WorkersPool) Start() {
	timer := time.NewTimer(time.Second) // контролирует балансировку каждую секунду
	defer timer.Stop()

	// при старте пула создаем минимальное число воркеров
	for i := 0; i < wp.minNum; i++ {
		wp.createNewWorker()
	}

	// в цикле запускаем балансировку кол-ва воркеров
	for {
		select {
		case <-timer.C:
			if wp.globalStop.Load() {
				fmt.Printf("WorkerPool stopped balancing\nWaiting for all workers to finish...\n")
				return
			}

			wp.balanceWorkersNum()
			timer.Reset(time.Second)
		}
	}
}
