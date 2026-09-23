package main

import (
	"fmt"
	"sync"
	"time"
)

type Race struct {
	cond  *sync.Cond
	ready bool
}

func NewRace() *Race {
	return &Race{
		cond:  sync.NewCond(new(sync.Mutex)),
		ready: false,
	}
}

func waiter(race *Race, name string) {
	fmt.Printf("%s started\n", name)

	race.cond.L.Lock()
	fmt.Printf("%s is waiting\n", name)
	for !race.ready { // защита от случайного просыпания
		race.cond.Wait()
		fmt.Printf("%s is ready\n", name)
	}
	race.cond.L.Unlock()        // нужен потому что wait при срабатывании закрывает Lock
	time.Sleep(2 * time.Second) // не должно быть внутри структуры выше
	fmt.Printf("%s is done\n", name)
}

func cooker(race *Race) {
	fmt.Printf("Cooker started\n")
	time.Sleep(5 * time.Second)
	race.ready = true
	race.cond.Broadcast()
	fmt.Printf("Cooker is done\n")
}

func main() {
	race := NewRace()
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("Waiter-%d", i)
		wg.Go(func() {
			waiter(race, name)
		})
	}
	cooker(race)
	wg.Wait()
	fmt.Printf("All goroutines finished\n")
}
