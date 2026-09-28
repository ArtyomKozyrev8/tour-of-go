package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func bridge(ctx context.Context, inChans chan chan string) chan string {
	outChan := make(chan string)
	go func() {
		defer close(outChan)
		for inChan := range inChans {
		labelOne:
			for {
				select {
				case <-ctx.Done():
					return
				case val, ok := <-inChan:
					if !ok {
						break labelOne // обычный break внутри select выходит лишь из самого select
					}
					select {
					case <-ctx.Done():
						return
					case outChan <- val:
					}
				}
			}
		}
	}()
	return outChan
}

func creatorStrings(ctx context.Context, prefix string) chan string {
	chanOut := make(chan string)
	go func() {
		defer close(chanOut)
		for i := 0; i < 50; i++ {
			select {
			case <-ctx.Done():
				return
			case chanOut <- fmt.Sprintf("%s-%d", prefix, i):
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()
	return chanOut
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	inChans := make(chan chan string, 5) // буффер с запасом чтобы полжить все каналы
	inChans <- creatorStrings(ctx, "One")
	inChans <- creatorStrings(ctx, "Two")
	inChans <- creatorStrings(ctx, "Three")
	inChans <- creatorStrings(ctx, "Four")
	close(inChans)

	for val := range bridge(ctx, inChans) {
		fmt.Println(val)
	}

}
