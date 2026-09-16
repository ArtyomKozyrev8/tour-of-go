package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	producersNumber    = 6
	minConsumersNumber = 2
	maxConsumersNumber = 8
	workTimeSeconds    = 180
	bucketSize         = 26
)

type MyBucket struct {
	bucketSize int
	interval   int
	waitChan   chan struct{}
	timer      *time.Timer
}

func NewMyBucket(bucketSize int, interval int) *MyBucket {
	timer := time.NewTimer(time.Duration(interval) * time.Second)

	bucket := &MyBucket{
		bucketSize: bucketSize,
		interval:   interval,
		waitChan:   make(chan struct{}, bucketSize),
		timer:      timer,
	}
	for i := 0; i < bucket.bucketSize; i++ {
		bucket.waitChan <- struct{}{}
	}
	return bucket
}

func (bucket *MyBucket) Start() {
	go func() {
		for {
			select {
			case <-bucket.timer.C:
			labelOne:
				for i := 0; i < bucket.bucketSize; i++ {
					select {
					case bucket.waitChan <- struct{}{}:
						continue
					default:
						break labelOne
					}

				}
				bucket.timer.Reset(time.Duration(bucket.interval) * time.Second)
			}
		}
	}()
}

func (bucket *MyBucket) Wait() struct{} {
	return <-bucket.waitChan
}

func (bucket *MyBucket) GetNoWait() bool {
	select {
	case <-bucket.waitChan:
		return true
	default:
		return false
	}
}

type MetricsTaskCounter struct {
	producerCreated      atomic.Int64
	producerSent         atomic.Int64
	producerLost         atomic.Int64
	producerDropped      atomic.Int64
	dispatcherDispatched atomic.Int64
	dispatcherLost       atomic.Int64
	consumerFinished     atomic.Int64
}

func (t *MetricsTaskCounter) String() string {
	return fmt.Sprintf(
		"MetricsTaskCounter{producerCreated=%d, producerSent=%d, producerLost=%d, producerDropped=%d, dispatcherDispatched=%d, dispatcherLost=%d, consumerFinished=%d}",
		t.producerCreated.Load(),
		t.producerSent.Load(),
		t.producerLost.Load(),
		t.producerDropped.Load(),
		t.dispatcherDispatched.Load(),
		t.dispatcherLost.Load(),
		t.consumerFinished.Load(),
	)
}

type GlobalTask struct {
	task atomic.Int64
}

type ContextWithCancel struct {
	ctx          context.Context
	cancel       context.CancelFunc
	consumerName string
}

func producer(
	ctx context.Context,
	producerCh chan<- int,
	producerName string,
	metrics *MetricsTaskCounter,
	index *GlobalTask,
	bucket *MyBucket,
) {

	// создаем таймер один раз на цикл, чтобы не текла память
	timer := time.NewTimer(time.Millisecond * (600 + time.Duration(rand.Intn(400))))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("%s is canceled\n", producerName)
			return
		case <-timer.C:
			task := int(index.task.Add(1))
			metrics.producerCreated.Add(1)
			if bucket.GetNoWait() {
				select {
				case producerCh <- task:
					metrics.producerSent.Add(1) // отправляем задачу в диспетчер
				default:
					metrics.producerLost.Add(1) // не смог отправить сразу задачу
				}
			} else {
				metrics.producerDropped.Add(1) // не смог отправить сразу задачу
			}

			timer.Reset(time.Millisecond * (300 + time.Duration(rand.Intn(300))))
		}
	}
}

func cleanDispatcher(producerCh chan int, consumerCh chan<- int, metrics *MetricsTaskCounter, wgProducers *sync.WaitGroup) {
	wgProducers.Wait()
	close(producerCh)

	for task := range producerCh {
		consumerCh <- task
		fmt.Printf("Task-%d was dispatched while cleaning\n", task)
		metrics.dispatcherDispatched.Add(1)
	}
	close(consumerCh)

	fmt.Printf("dispatcher cleaned!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
}

func dispatcher(
	ctx context.Context,
	wg *sync.WaitGroup,
	producerCh chan int,
	consumerCh chan int,
	metrics *MetricsTaskCounter,
	wgProducers *sync.WaitGroup) {

	localCounter := 0
	timer := time.NewTimer(time.Second)

	consumersCtx := make([]ContextWithCancel, 0)

	defer func() {
		cleanDispatcher(producerCh, consumerCh, metrics, wgProducers)
	}()

	for i := 0; i < minConsumersNumber; i++ {
		killCtx, cancel := context.WithCancel(context.Background()) // создаем отдельный контекст
		num := len(consumersCtx) + 1
		name := fmt.Sprintf("Consumer-%d-%d", num, 100+rand.Intn(500))
		consumersCtx = append(consumersCtx, ContextWithCancel{killCtx, cancel, name})

		wg.Go(func() {
			consumer(killCtx, consumerCh, metrics, name)
		})
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Dispatcher is canceled\n")
			return
		case task := <-producerCh:
			localCounter++
			consumerCh <- task
			// fmt.Printf("Task-%d was dispatched\n", task)
			metrics.dispatcherDispatched.Add(1)
		case <-timer.C:
			if localCounter > 15 {
				if len(consumersCtx) < maxConsumersNumber {
					killCtx, cancel := context.WithCancel(context.Background())
					num := len(consumersCtx) + 1
					name := fmt.Sprintf("Consumer-%d-%d", num, 100+rand.Intn(500))
					consumersCtx = append(consumersCtx, ContextWithCancel{killCtx, cancel, name})
					fmt.Printf("Consumers number increased %d\n", len(consumersCtx))
					wg.Go(func() {
						consumer(
							killCtx,
							consumerCh,
							metrics,
							name)
					})

				}
			} else if localCounter < 10 {
				if len(consumersCtx) > minConsumersNumber {
					ctxWithCancel := consumersCtx[len(consumersCtx)-1]
					consumersCtx = consumersCtx[:len(consumersCtx)-1]
					fmt.Printf("Consumers number decreased %d\n", len(consumersCtx))
					ctxWithCancel.cancel()
				}
			} else {
				// ничего не делаем
			}
			timer.Reset(time.Second)
			fmt.Printf("RPS: %d\n", localCounter)
			localCounter = 0
		}
	}
}

func consumer(ctx context.Context, consumerCh <-chan int, metrics *MetricsTaskCounter, consumerName string) {
	fmt.Printf("%s is created\n", consumerName)
	for {
		// вложенные select c приоритетом !
		select {
		case <-ctx.Done():
			fmt.Printf("%s was canceled\n", consumerName)
			return
		default:
			select {
			case _, ok := <-consumerCh:
				if !ok {
					fmt.Printf("%s all tasks were done\n", consumerName)
					return
				}
				// fmt.Printf("%s: Task-%d was consumed\n", consumerName, task)
				time.Sleep(time.Millisecond * (600 + time.Duration(rand.Intn(400))))
				metrics.consumerFinished.Add(1)
			case <-ctx.Done():
				fmt.Printf("%s was canceled\n", consumerName)
				return
			}
		}
	}
}

func main() {
	notifyCtx, cancelNotify := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelNotify()

	ctx, cancel := context.WithTimeout(notifyCtx, time.Duration(workTimeSeconds)*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	producerCh := make(chan int, 1000) // задачи для диспетчера от продьюсеров
	consumerCh := make(chan int, 1000) // задачи для консьюмеров от диспетчера

	metrics := &MetricsTaskCounter{}
	index := &GlobalTask{}
	bucket := NewMyBucket(bucketSize, 1)
	bucket.Start()

	stopContext, stopContextCancel := context.WithCancel(context.Background())

	go func(ctx context.Context, metrics *MetricsTaskCounter) {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Println(metrics)
			case <-ctx.Done():
				return
			}
		}
	}(stopContext, metrics)

	wgProducers := sync.WaitGroup{}

	for i := 0; i < producersNumber; i++ {
		wgProducers.Go(func() {
			producer(ctx, producerCh, fmt.Sprintf("Producer-%d", i+1), metrics, index, bucket)
		})
	}

	wg.Go(func() {
		dispatcher(ctx, wg, producerCh, consumerCh, metrics, &wgProducers)
	})

	wg.Wait()
	fmt.Printf("All work is done!!\n")
	stopContextCancel()
	fmt.Println(metrics)
}
