package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	countNums = 100
)

// generator отправляет числа от 0 до countNums в канал
func generator(ctx context.Context) chan int {
	out := make(chan int, countNums)

	go func() {
		defer close(out)

		for i := 0; i < countNums; i++ {
			select {
			case <-ctx.Done():
				fmt.Printf("\ngenerator завершён по отмене контекста.\n")
				return
			case out <- i:
			}
		}

		fmt.Printf("\ngenerator завершил отправку.\n")
	}()

	return out
}

// process обрабатывает числа из in, например, возводит в квадрат и печатает.
// Приём done-канала позволяет явно завершить горутину извне.
// Возвращает doneCh, который закрывается после полного завершения горутины.
func process(ctx context.Context, in chan int, done chan struct{}) chan struct{} {
	doneCh := make(chan struct{})

	go func() {
		defer func() {
			close(doneCh) // когда тут закрываем doneCh, то в main() мы ловим zero value
			fmt.Println("process завершён.")
		}()

		for {
			select {
			case <-ctx.Done():
				fmt.Printf("process завершается по отмене контекста.\n")
				return
			case <-done: // если done закроется в main(), то тут мы поймаем zero value
				fmt.Printf("process завершается по сигналу done-канала.\n")
				return
			case v, ok := <-in:
				if !ok {
					fmt.Printf("входящий канал закрыт, перестаём его слушать. process завершается.\n")
					return
				}

				fmt.Printf("v*v = %d\n", v*v)
				time.Sleep(150 * time.Millisecond) // создаём вид бурной деятельности
			}
		}
	}()

	return doneCh
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	// получаем канал с данными для process
	numsCh := generator(ctx)

	// done - это канал для подачи сигнала завершения горутине.
	// Пустые структуры используем потому, что они мало весят в силу оптимизаций Go.
	done := make(chan struct{})

	// doneCh - это канал для подачи сгнала main() о том, что горутина завершилась
	doneCh := process(ctx, numsCh, done)

	// даём программе немного поработать и явно закрываем done-канал
	time.Sleep(1 * time.Second)
	fmt.Println("закрываем done-канал")
	close(done)

	// здесь блокируемся и ждём обратную связь
	// от process о её реальном завершении
	<-doneCh

	cancel()
	wg.Wait()

	fmt.Println("Программа завершена.")
}

// signalHandler слушает сигналы отмены
func signalHandler(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer wg.Done()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)

	select {
	case <-ctx.Done():
		fmt.Println("\nsignalHandler завершается по отмене контекста.")
		return
	case <-sig:
		cancel()
		fmt.Println("\nsignalHandler завершается по сигналу отмены.")
		return
	}
}
