package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

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

// generator отправляет числа от 0 до n в канал
func generator(ctx context.Context, n int) chan int {

	out := make(chan int)

	go func() {
		defer close(out)
		for i := range n {
			select {
			case <-ctx.Done():
				fmt.Println("\ngenerator завершён по отмене контекста.")
				return
			case out <- i:
			}
		}
		fmt.Println("\ngenerator завершил отправку.")
	}()

	return out
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	for v := range generator(ctx, 100000) {
		fmt.Print(v, " ")
	}

	wg.Wait()
}
