package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

func prepareString(input string, pool *sync.Pool) string {
	storage := pool.Get().(*[]rune) // приведение типа из пула
	localStorage := *storage        // получаем значение

	defer func() {
		localStorage = localStorage[:0] // отматываем на начало слайса
		*storage = localStorage         // присваиваем новое значения ячейке памяти
		pool.Put(storage)
	}()

	for _, letter := range input {
		if letter != '_' {
			localStorage = append(localStorage, letter)
		}
	}

	time.Sleep(1500 * time.Millisecond)
	return string(localStorage)
}

func consumer(ctx context.Context, wg *sync.WaitGroup, inChan <-chan string, outChan chan<- string, pool *sync.Pool, name string) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("%s is done due to cancel\n", name)
			return
		case in, ok := <-inChan:
			if !ok {
				fmt.Printf("%s is done due to no more tasks\n", name)
				return
			}
			outMessage := prepareString(in, pool)
			select {
			case <-ctx.Done():
				fmt.Printf("%s is done due to cancel\n", name)
				return
			case outChan <- outMessage:
			}
		}
	}
}

func producer(ctx context.Context, inString *[]string, inChan chan<- string) {
	defer close(inChan)
	temp := *inString
	for _, in := range temp {
		select {
		case <-ctx.Done():
			fmt.Printf("Producer is done due to cancel\n")
			return
		case inChan <- in:
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	pool := sync.Pool{
		New: func() any {
			storage := make([]rune, 0, 1024)
			return &storage
		},
	}
	wg := &sync.WaitGroup{}
	inChan := make(chan string)
	outChan := make(chan string)

	arrayStrings := []string{
		"hello__world_1",
		"hello___world_2",
		"hello___world_3",
		"hello___world_4",
		"hello______world___5",
		"hello___world___6",
		"hello____world____7",
		"hello___world___8",
		"hello___world___9",
	}

	go producer(ctx, &arrayStrings, inChan)
	for i := 1; i <= 4; i++ {
		wg.Add(1)
		name := fmt.Sprintf("consumer-%d", i)
		go consumer(ctx, wg, inChan, outChan, &pool, name)
	}

	go func() {
		wg.Wait()
		close(outChan)
		fmt.Println("All workers done")
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Main work is cancelled\n")
			return
		case message, ok := <-outChan:
			if !ok {
				fmt.Printf("All tasks are done\n")
				return
			}
			time.Sleep(100 * time.Millisecond)
			fmt.Println(message)
		}
	}
}
