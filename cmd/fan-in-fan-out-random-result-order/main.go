/*
Эта вариация Fan-In / Fan-Out отличается от КЛАССИЧЕСКОГО воркер-пула (producers-consumers) тем,
что вместо одного общего канала результатов, у каждого продюсера есть свой персональный выходной канал.

Затем этот список каналов передается в консьюмер-мультиплексор, который запускает фоновые горутины,
одновременно читает из всех каналов и сливает данные в одну общую кучу без сохранения исходного порядка (кто быстрее).
*/

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

const taskNumber = 100

func taskDistributor(ctx context.Context) chan int {
	chanDistributor := make(chan int)

	go func() {
		defer close(chanDistributor)
		defer fmt.Println("taskDistributor finished")
		fmt.Println("taskDistributor started")
		for i := 0; i < taskNumber; i++ {
			select {
			case <-ctx.Done():
				fmt.Println("taskDistributor was cancelled")
				return
			case chanDistributor <- i:
			}
		}
	}()

	return chanDistributor
}

func processTask(task int, consumerName string) string {
	time.Sleep(100 * time.Millisecond)
	return fmt.Sprintf("cName-%s-task-%d", consumerName, task)
}

func consumer(ctx context.Context, consumersChan <-chan int, name string) <-chan string {
	outChan := make(chan string)

	go func() {
		defer close(outChan)
		fmt.Printf("consumer %s started\n", name)
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("consumer %s canceled\n", name)
				return
			case task, ok := <-consumersChan:
				if !ok {
					fmt.Printf("consumer %s finished\n", name)
					return
				}
				select {
				case <-ctx.Done():
					fmt.Printf("consumer %s canceled\n", name)
					return
				case outChan <- processTask(task, name):
				}
			}
		}
	}()
	return outChan
}

func multiplexer(ctx context.Context, wg *sync.WaitGroup, consumersChan ...<-chan string) chan string {
	resultChan := make(chan string)

	for i, c := range consumersChan {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("multiplexer-%d canceled\n", i)
					return
				case task, ok := <-c:
					if !ok {
						fmt.Printf("multiplexer-%d finished\n", i)
						return
					}
					select {
					case <-ctx.Done():
						fmt.Printf("multiplexer-%d canceled\n", i)
						return
					case resultChan <- task:
					}
				}
			}
		})
	}

	go func() {
		//  ожидаем когда все мультиплексоры выкобчаться и закрываем канал результатов
		wg.Wait()
		close(resultChan)
	}()

	return resultChan
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	distributorChan := taskDistributor(ctx)

	chanConsumerOne := consumer(ctx, distributorChan, "One")
	chanConsumerTwo := consumer(ctx, distributorChan, "Two")
	chanConsumerThree := consumer(ctx, distributorChan, "Three")
	chanConsumerFour := consumer(ctx, distributorChan, "Four")

	wgMultiplexer := sync.WaitGroup{}
	resultChan := multiplexer(ctx, &wgMultiplexer, chanConsumerOne, chanConsumerTwo, chanConsumerThree, chanConsumerFour)
	for result := range resultChan {
		fmt.Println(result)
	}
	fmt.Println("All work is done!")
}
