package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

type ConsumerResult struct {
	result string
	error  error
}

func prepareString(input string, pool *sync.Pool) (string, error) {
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
	res := string(localStorage)

	if len(res) == 0 {
		return "", errors.New("empty result")
	}

	return res, nil
}

func consumer(
	ctx context.Context,
	wg *sync.WaitGroup,
	inChan <-chan string,
	outChan chan<- *ConsumerResult,
	pool *sync.Pool,
	name string) {
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
			outMessage, err := prepareString(in, pool)
			// запись &ConsumerResult - чтобы в куче выделялась новая память под каждую операцию
			// если написать result := ConsumerResult{outMessage, err}, а потом в канал передавать
			// &res это может прривести к ошибкам, так как при каждом срабатывании консьюмера
			// в стеке может переиспользоваться тот же участок памяти что приведет к ошибкам
			var errConsumer error = nil
			if err != nil {
				errConsumer = fmt.Errorf("error preparing message in %s. Details: %w", name, err)
			}

			result := &ConsumerResult{outMessage, errConsumer}
			select {
			case <-ctx.Done():
				fmt.Printf("%s is done due to cancel\n", name)
				return
			case outChan <- result:
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
	// по значению в данном случае передача быстрее (маленький объект)
	//и безопаснее (создается новый объект)
	// передаем по ссылке для опыта (будет эффективно при передаче больших объектов)
	outChan := make(chan *ConsumerResult)

	arrayStrings := []string{
		"hello__world_1",
		"hello___world_2",
		"hello___world_3",
		"hello___world_4",
		"_____",
		"hello______world___5",
		"hello___world___6",
		"hello____world____7",
		"hello___world___8",
		"____________",
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
		case result, ok := <-outChan:
			if !ok {
				fmt.Printf("All tasks are done\n")
				return
			}
			time.Sleep(100 * time.Millisecond)
			if result.error != nil {
				fmt.Printf("Error: %s\n", result.error)
			} else {
				fmt.Printf("Result: %s\n", result.result)
			}
		}
	}
}
