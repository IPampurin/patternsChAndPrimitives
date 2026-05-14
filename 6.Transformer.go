package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	countNums = 100
)

// generator отправляет числа от 0 до countNums в канал
func generator(ctx context.Context) chan int {

	out := make(chan int)

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

// transformer изменяет число из входящего канала по оправилу action и отправляет результат в исходящий канал
func transformer(ctx context.Context, in chan int, action func(int) int) chan int {

	res := make(chan int)

	go func() {

		defer func() {
			close(res)
			fmt.Println("transformer завершён.")
		}()

		for {
			select {
			case <-ctx.Done():
				fmt.Printf("transformer завершается по отмене контекста.\n")
				return
			case v, ok := <-in:
				if !ok {
					fmt.Printf("входящий канал закрыт, перестаём его слушать. transformer завершается.\n")
					return
				}

				select {
				case <-ctx.Done():
					fmt.Printf("transformer завершается по отмене контекста.\n")
					return
				case res <- action(v):
				}
			}
		}
	}()

	return res
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	numsCh := generator(ctx)
	action := func(num int) int {
		return num * 2
	}

	nums := make([]int, 0)
	for v := range transformer(ctx, numsCh, action) {
		nums = append(nums, v)
	}

	fmt.Println(nums)

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
