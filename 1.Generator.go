package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// signalHandler слушает сигналы отмены,
// например, нажатие Ctrl+C
func signalHandler(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	// помним, что при выходе из signalHandler надо декрементировать счётчик WaitGroup
	defer wg.Done()

	// создаём канал для прослушки сигналов ОС
	// SIGINT - Ctrl+C в терминале
	// SIGTERM - сигнал по умолчанию для команды kill, например,
	// если Docker решил остановить контейнер, то он пришлёт SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig) // для порядка делаем Stop каналу

	// в селекте будем ждать одного из двух событий:
	// срабатывания cancel из контекста или сигнала от ОС
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
	// сначала создадим то, что хотим возвращать из функции
	out := make(chan int)

	// а вот эта горутина будет выполняться фоном уже после
	// того, как мы в return-е функции generator вернём канал
	go func() {
		// помним, что отправитель каналы должен закрывать
		defer close(out)

		// перебираем числа и либо шлём в канал, либо завершаем горутину по отмене контекста
		for i := range n {
			select {
			case <-ctx.Done():
				fmt.Println("\ngenerator завершён по отмене контекста.")
				return
			case out <- i:
			}
		}
		// логируем событие
		fmt.Println("\ngenerator завершил отправку.")
	}()

	return out // отдаём созданный в начале функции канал
}

func main() {
	// создаём контекст для того, чтобы через cancel
	// иметь возможность остановить какие бы то ни было горутины
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// WaitGroup со своим счётчиком понадобится, чтобы дождаться
	// завершения нужного количества горутин
	var wg sync.WaitGroup

	// добавим в счётчик одну горутину, в данном случае signalHandler,
	// используемый для фоновой обработки сигналов отмены
	wg.Add(1)
	go signalHandler(ctx, cancel, &wg)

	// тут мы блокируемся на прослушивании канала, который возвращает generator,
	// до тех пор, пока этот самый канал не закроет отправитель
	for v := range generator(ctx, 100000) {
		fmt.Print(v, " ")
	}

	wg.Wait() // дожидаемся завершения горутин, для которых WaitGroup вела счётчик
}
