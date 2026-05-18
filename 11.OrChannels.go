package main

import (
	"fmt"
	"time"
)

// or объединяет несколько каналов и возвращает один,
// который закроется при закрытии любого из переданных
func or(channels ...chan interface{}) chan interface{} {

	switch len(channels) {
	case 0:
		return nil
	case 1:
		return channels[0]
	}

	// результирующий канал
	done := make(chan interface{})

	go func() {
		defer close(done) // закрываем при выходе из горутины

		select {
		case <-channels[0]: // ждём первый канал
			return
		case <-or(channels[1:]...): // рекурсивно ждём остальные
			return
		}
	}()

	return done
}

func main() {

	start := time.Now()

	// вспомогательная функция для создания канала, который закроется через заданное время
	sig := func(after time.Duration) chan interface{} {
		c := make(chan interface{})
		go func() {
			defer close(c)
			time.Sleep(after)
		}()
		return c
	}

	// ждём первый сработавший таймер (самый короткий - 1 секунда)
	<-or(
		sig(2*time.Hour),
		sig(5*time.Minute),
		sig(1*time.Second),
		sig(1*time.Hour),
		sig(1*time.Minute),
	)

	finish := time.Since(start)

	fmt.Printf("Программа завершена через %s\n", finish)
}
