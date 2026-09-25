package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
)

func multiply(ctx context.Context, inChan <-chan int, multiplier int) chan int {
	outChan := make(chan int)
	go func() {
		defer close(outChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case in, ok := <-inChan:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case outChan <- in * multiplier:
					}
				}

			}
		}
	}()
	return outChan
}

func sum(ctx context.Context, inChan <-chan int, adder int) chan int {
	outChan := make(chan int)
	go func() {
		defer close(outChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case in, ok := <-inChan:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case outChan <- in + adder:
					}
				}

			}
		}
	}()
	return outChan
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 1; i <= 10; i++ {
			ch <- i
		}
	}()
	for val := range multiply(ctx, multiply(ctx, sum(ctx, ch, 5), 2), 2) {
		fmt.Printf("%d ", val)
	}
}
