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

// filter фильтрует числа из входящего канала по правилу condition и отправляет результат в исходящий канал
func filter(ctx context.Context, in chan int, condition func(int) bool) chan int {
	res := make(chan int)

	// фоновая горутина, как и в предыдущих паттернах, получает данные из канала
	// и отправляет дальше просто после проверки "фильтрующего" условия
	go func() {
		defer func() {
			close(res)
			fmt.Println("filter завершён.")
		}()

		for {
			select {
			case <-ctx.Done():
				fmt.Printf("filter завершается по отмене контекста.\n")
				return
			case v, ok := <-in:
				if !ok {
					fmt.Printf("входящий канал закрыт, перестаём его слушать. filter завершается.\n")
					return
				}

				if condition(v) {
					select {
					case <-ctx.Done():
						fmt.Printf("filter завершается по отмене контекста.\n")
						return
					case res <- v:
					}
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

	// определяем условие, по которому будем производить фильтрацию
	condition := func(num int) bool {
		if num%2 == 0 {
			return true
		}

		return false
	}

	nums := make([]int, 0)
	for v := range filter(ctx, numsCh, condition) {
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
