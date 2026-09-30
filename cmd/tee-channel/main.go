/*
Tee-канал — это паттерн конкурентного проектирования в Go, который принимает один входящий поток данных и дублирует
его в два (или более) независимых исходящих канала. Название взято по аналогии с сантехническим тройником (T-splitter)
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

func tee[T any](ctx context.Context, inChan <-chan T) (<-chan T, <-chan T) {
	chOne := make(chan T)
	chTwo := make(chan T)
	go func() {
		defer close(chOne)
		defer close(chTwo)
		for {
			select {
			case <-ctx.Done():
				return
			case t, ok := <-inChan:
				if !ok {
					return
				}

				one, two := chOne, chTwo
				// внутри select используется ctx.Done таким образом нет гарантии, что сообщение попадет в оба канала
				// мы присваием значения one и two в nil потому что в случае select запись/чтение в nil канал
				// всегда блокируется и этот case никогда не выполняется
				for one != nil || two != nil {
					select {
					case <-ctx.Done():
						return
					case one <- t:
						one = nil
					case two <- t:
						two = nil
					}
				}
			}
		}
	}()

	return chOne, chTwo
}

func feeder(ctx context.Context) <-chan int {
	outChan := make(chan int)

	go func() {
		defer close(outChan)
		for i := 0; i < 50; i++ {
			time.Sleep(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				return
			case outChan <- i:
			}
		}
	}()

	return outChan
}

func consumer(ctx context.Context, ch <-chan int, name string) {
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-ch:
			if !ok {
				return
			}
			fmt.Printf("%s-%d\n", name, t)
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	wg := sync.WaitGroup{}
	chOne, chTwo := tee(ctx, feeder(ctx))
	wg.Go(func() {
		consumer(ctx, chOne, "One")
	})
	wg.Go(func() {
		consumer(ctx, chTwo, "Two")
	})
	wg.Wait()
}
