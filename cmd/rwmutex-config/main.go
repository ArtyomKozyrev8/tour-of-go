package main

import (
	"fmt"
	"sync"
	"time"
)

type RWConfig struct {
	// RWMutex - подходит для использования в системах где более 90% запросов это чтение
	rwMutex sync.RWMutex
	storage map[string]interface{}
}

func NewRWConfig() *RWConfig {
	return &RWConfig{
		storage: make(map[string]interface{}),
	}
}

func (rwc *RWConfig) Get(field string) interface{} {
	rwc.rwMutex.RLock()
	defer rwc.rwMutex.RUnlock()
	v, _ := rwc.storage[field]
	return v
}

func (rwc *RWConfig) Put(field string, value interface{}) {
	rwc.rwMutex.Lock()
	defer rwc.rwMutex.Unlock()
	rwc.storage[field] = value
}

func (rwc *RWConfig) Delete(field string) {
	rwc.rwMutex.Lock()
	defer rwc.rwMutex.Unlock()
	delete(rwc.storage, field)
}

func someReader(config *RWConfig, readerName string) {
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		fmt.Printf("readerName: fieldA = %s\n", config.Get("fieldA"))
	}
}

func someWriter(config *RWConfig) {
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second * 2)
		config.Put("fieldA", i)
	}
}

func main() {
	config := NewRWConfig()
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Go(func() {
			someWriter(config)
		})
		readerName := fmt.Sprintf("Reader-%d", i+1)
		wg.Go(func() {
			someReader(config, readerName)
		})
	}
	wg.Wait()
}
