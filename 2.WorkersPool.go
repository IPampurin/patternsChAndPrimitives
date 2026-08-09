package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// в const зададим начальные условия
const (
	maxNumToProcess = 1000000 // количество обрабатываемых чисел
	countWorkers    = 5       // количество воркеров в пуле
)

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

// workersPool слушает канал и выполняет работу до закрытия канала или отмены контекста
func workersPool(ctx context.Context, countWorkers int, in chan int) chan int {
	// сначала создадим то, что хотим возвращать из функции
	out := make(chan int)

	// WaitGroup со своим счётчиком понадобится, чтобы дождаться
	// завершения нужного количества горутин.
	// Обратим внимание, что эта WaitGroup совершенно другая, нежели в main()
	var workersWg sync.WaitGroup

	// создаём желаемое количество горутин-воркеров
	for i := 0; i < countWorkers; i++ {
		workersWg.Add(1) // до цикла можно было бы сделать и так workersWg.Add(countWorkers)

		// в самом *фоновом* воркере мы лишь в бесконечном цикле
		// либо получаем сигнал отмены,
		// либо получаем данные для обработки
		go func() {
			defer workersWg.Done() // помним про декрементацию счётчика

			for {
				select {
				// либо получаем сигнал отмены
				case <-ctx.Done():
					fmt.Printf("воркер %d завершён по отмене контекста.\n", i)
					return
				// либо получаем данные для обработки - используем синтаксис v, ok := <-in,
				// чтобы не ловить zero value из закрытого канала
				case v, ok := <-in:
					if !ok {
						fmt.Printf("воркер %d завершён по закрытию входного канала.\n", i)
						return
					}

					result := v * 2 // создаём вид бурной деятельности, что-то очень долго считаем

					// ну а селект в селекте нужен, чтобы перед отправкой результата
					// проверить не завершается ли наша программа, пока мы получали результат
					select {
					// либо получаем сигнал отмены
					case <-ctx.Done():
						fmt.Printf("в воркере %d передача данных прервана по отмене контекста, воркер завершён.\n", i)
						return
					// либо пытаемся отправить результат
					case out <- result:
					}
				}
			}
		}()
	}

	// фоном ожидаем завершения воркеров и,
	// когда в него уже никто не пишет, закрываем канал с результатами
	go func() {
		workersWg.Wait()
		close(out)
	}()

	return out // отдаём созданный в начале функции канал
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	// получаем канал с данными из генератора
	numsChan := generator(ctx, maxNumToProcess)
	// получаем канал с результатами от воркеров
	resChan := workersPool(ctx, countWorkers, numsChan)

	// блокируемся на получении результатов
	for v := range resChan {
		if v%10000 == 0 {
			fmt.Println(v)
		}
	}

	// после закрытия resChan отменяем контекст и дожидаемся всех
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
