package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	countOutChans = 5   // на сколько каналов будем делить канал
	numCount      = 100 // сколько чисел будем отправлять в исходный канал
)

// generator отправляет числа от 0 до n в канал
func generator(ctx context.Context, n int) chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for i := 0; i < n; i++ {
			// для наглядности результата число для отправки выбираем
			// псевдослучайным образом из диапазона от 0 до countOutChans
			num := rand.IntN(countOutChans)
			select {
			case <-ctx.Done():
				fmt.Printf("\ngenerator завершён по отмене контекста.\n")
				return
			case out <- num:
			}
		}
		fmt.Println("\ngenerator завершил отправку.")
	}()

	return out
}

// fanOut делит входящий канал на countOutChans каналов
func fanOut(ctx context.Context, in chan int) []chan int {
	// сначала создадим то, что хотим возвращать из функции - слайс каналов ёмкостью countOutChans,
	// а в цикле - инициализируем каждый канал слайса
	chs := make([]chan int, countOutChans)
	for i := range chs {
		chs[i] = make(chan int)
	}

	// в *фоновой* горутине-воркере мы лишь в бесконечном цикле
	// либо получаем сигнал отмены,
	// либо получаем данные для обработки
	go func() {
		// при выходе из горутины надо пройти по слайсу и закрыть каждый из каналов
		defer func() {
			for i := range chs {
				close(chs[i])
			}
			fmt.Println("fanOut завершён.")
		}()

		for {
			select {
			// либо получаем сигнал отмены
			case <-ctx.Done():
				fmt.Printf("fanOut завершается по отмене контекста.\n")
				return
			// либо получаем данные для обработки - используем синтаксис v, ok := <-in,
			// чтобы не ловить zero value из закрытого канала
			case v, ok := <-in:
				if !ok {
					fmt.Printf("входящий канал закрыт, перестаём его слушать. fanOut завершается.\n")
					return
				}

				// здесь проверяем не случилось ли отмены и действуем аналогичным образом
				select {
				// либо получаем сигнал отмены
				case <-ctx.Done():
					fmt.Printf("fanOut завершается по отмене контекста до отправки в канал №%d.\n", v)
					return
				// либо пытаемся отправить результат
				case chs[v] <- v:
				}
			}
		}
	}()

	return chs // отдаём созданный в начале функции слайс каналов
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup // будет ждать signalHandler
	var mu sync.Mutex     // будем использовать при записи результатов для наглядности

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	// в res будут храниться слайсы с числами по значениям:
	// - res[0] хранит все отправленные нули
	// - res[1] хранит все отправленные единицы
	// и т.д.
	res := make([][]int, countOutChans)
	for i := range res {
		res[i] = make([]int, 0)
	}

	in := generator(ctx, numCount) // получаем канал для разделения
	chs := fanOut(ctx, in)         // получаем каналы после разделения

	// результаты будем собирать отдав каждый из результирующих каналов отдельной горутине,
	// слайсы для сбора чисел при этом не привязаны к конкретной горутине
	var readersWg sync.WaitGroup // используем для ожидания горутин сбора результатов
	for i := 0; i < countOutChans; i++ {
		readersWg.Add(1)
		go func() {
			defer readersWg.Done()
			for v := range chs[i] {
				// так как несколько горутин могут получить одно и то же v,
				// то записываем под мьютексом
				mu.Lock()
				res[v] = append(res[v], v)
				mu.Unlock()
			}
		}()
	}

	readersWg.Wait() // дожидаемся читателей
	cancel()         // сигнализируем каким бы то ни было горутинам закругляться
	wg.Wait()        // дожидаемся (в данном случае только signalHandler)

	// выводим результат
	for i := range res {
		fmt.Println(res[i])
	}
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
