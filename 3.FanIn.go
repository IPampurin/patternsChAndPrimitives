package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// generator отправляет числа от n до m в канал
func generator(ctx context.Context, n, m, id int) chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for i := n; i < m; i++ {
			select {
			case <-ctx.Done():
				fmt.Printf("\ngenerator %d завершён по отмене контекста.\n", id)
				return
			case out <- i:
			}
		}
		fmt.Printf("\ngenerator %d завершил отправку.\n", id)
	}()

	return out
}

// fanIn объединяет ряд каналов в один
func fanIn(ctx context.Context, chs ...chan int) chan int {
	// сначала создадим то, что хотим возвращать из функции
	out := make(chan int)
	n := len(chs)

	var fanInWg sync.WaitGroup

	fanInWg.Add(n)
	// создаём столько горутин, сколько каналов, и даём каждой горуине слушать по каналу,
	// а в каждом фоновой горутине-воркере просто в бесконечном цикле
	// либо получаем сигнал отмены,
	// либо получаем данные для обработки
	for i := 0; i < n; i++ {
		go func() {
			defer fanInWg.Done() // помним про декрементацию счётчика

			for {
				select {
				// либо получаем сигнал отмены
				case <-ctx.Done():
					return
				// либо получаем данные для обработки - используем синтаксис v, ok := <-in,
				// чтобы не ловить zero value из закрытого канала
				case v, ok := <-chs[i]:
					if !ok {
						fmt.Printf("канал %d закрыт, перестаём его слушать. fanIn завершается.\n", i)
						return
					}

					// здесь проверяем не случилось ли отмены и действуем аналогичным образом
					select {
					// либо получаем сигнал отмены
					case <-ctx.Done():
						return
					// либо пытаемся отправить результат
					case out <- v:
					}
				}
			}
		}()
	}

	// фоном ожидаем завершения воркеров и,
	// когда в него уже никто не пишет, закрываем канал с результатами
	go func() {
		fanInWg.Wait()
		close(out)
		fmt.Println("fanIn завершён.")
	}()

	return out
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	// получаем три канала с помощью генератора
	ch1 := generator(ctx, 0, 1000000, 1)
	ch2 := generator(ctx, 1000001, 2000000, 2)
	ch3 := generator(ctx, 2000001, 3000000, 3)

	// а тут блокируемся до закрытия на прослушивании ОДНОГО канала с результатами
	for v := range fanIn(ctx, ch1, ch2, ch3) {
		if v%10000 == 0 {
			fmt.Println(v)
		}
	}

	// после закрытия прослушиваемого канала
	// отменяем контекст и дожидаемся завершения signalHandler
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
