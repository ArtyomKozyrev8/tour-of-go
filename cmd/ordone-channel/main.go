/*
Паттерн orDone нужен для того, чтобы безопасно читать из канала в цикле и не ловить утечки горутин при отмене контекста.

Он работает как переходник: вы отдаете ему «опасный» канал и контекст, а функция запускает фоновую горутину,
которая сама следит за закрытием источника и сигналом отмены. Наружу возвращается новый канал.

Главный плюс — это чистота кода. Вместо того чтобы в каждом цикле писать громоздкие конструкции со select,
вы можете использовать простой и красивый for range. Он автоматически остановится, если закроется канал или если отменится контекст.

Цена паттерна — на каждое такое чтение создается одна дополнительная горутина-враппер, которая живет до конца работы цикла.
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

func orDone[T any](ctx context.Context, inChan chan T) chan T {
	newChan := make(chan T)

	go func() {
		defer close(newChan)
		for {
			select {
			case <-ctx.Done():
				fmt.Println("I closed the channel due to cancellation")
				return
			case t, ok := <-inChan:
				if !ok {
					fmt.Println("I closed the channel due to all tasks done")
					return
				}
				select {
				case <-ctx.Done():
					fmt.Println("I closed the channel due to cancellation")
					return
				case newChan <- t:
				}
			}
		}
	}()
	return newChan
}

func feeder(ctx context.Context, inChan chan string, name string) {
	f := func(n int) string {
		time.Sleep(time.Duration(100) * time.Millisecond)
		return fmt.Sprintf("%s-%d", name, n)
	}

	for i := 0; i < 100; i++ {
		result := f(i)
		select {
		case <-ctx.Done():
			return
		case inChan <- result:
		}

	}
}

func closer(inChan chan string, wg *sync.WaitGroup) {
	wg.Wait()
	close(inChan)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	wg := sync.WaitGroup{}

	someShan := make(chan string)
	wg.Go(func() { feeder(ctx, someShan, "One") })
	wg.Go(func() { feeder(ctx, someShan, "Two") })
	go closer(someShan, &wg)

	for task := range orDone(ctx, someShan) {
		fmt.Println(task)
	}
}
