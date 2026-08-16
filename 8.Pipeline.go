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
	countNums = 10
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

// processOne применяет к числам из входящего канала правило actionOne
// и отправляет результат в исходящий канал
func processOne(in chan int, actionOne func(int) int) chan int {
	res := make(chan int)

	go func() {
		defer func() {
			close(res)
			fmt.Println("processOne завершён.")
		}()

		// просто слушаем и отправляем дальше в надежде, что
		// у нашего небуферизированного канала есть читатель
		for v := range in {
			res <- actionOne(v)
		}
	}()

	return res
}

// processTwo применяет к числам из входящего канала правило actionTwo
// и отправляет результат в исходящий канал
func processTwo(in chan int, actionTwo func(int) int) chan int {
	res := make(chan int)

	go func() {
		defer func() {
			close(res)
			fmt.Println("processTwo завершён.")
		}()

		for v := range in {
			res <- actionTwo(v)
		}
	}()

	return res
}

// processThree применяет к числам из входящего канала правило actionThree
// и отправляет результат в исходящий канал
func processThree(in chan int, actionThree func(int) int) chan int {
	res := make(chan int)

	go func() {
		defer func() {
			close(res)
			fmt.Println("processThree завершён.")
		}()

		for v := range in {
			res <- actionThree(v)
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

	// определяем действия для каждой ступени пайплайна
	actoinOne := func(num int) int {
		return num + 2
	}
	actoinTwo := func(num int) int {
		return num - 2
	}
	actoinThree := func(num int) int {
		return num + 1
	}

	// в nums будем складывать результат
	nums := make([]int, 0)

	// получаем канал после первой ступени обработки исходных данных
	levelOne := processOne(generator(ctx), actoinOne)

	// получаем канал после второй ступени обработки
	levelTwo := processTwo(levelOne, actoinTwo)

	// получаем канал после третьей ступени обработки и записываем результат
	levelTree := processThree(levelTwo, actoinThree)
	for v := range levelTree {
		nums = append(nums, v)
	}

	cancel()
	wg.Wait()

	fmt.Println(nums)

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
