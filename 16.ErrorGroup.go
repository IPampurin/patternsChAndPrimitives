package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ErrGroup реализует группу горутин, которая отслеживает первую возникшую ошибку
// и предоставляет сигнал отмены остальным через закрытие doneCh
type ErrGroup struct {
	err    error          // первая (и единственная) пойманная ошибка
	wg     sync.WaitGroup // позволяет дождаться все горутины в группе
	once   sync.Once      // гарантирует, что doneCh закроется только один раз
	doneCh chan struct{}  // сигнальный канал: закрывается при первой ошибке
}

// NewErrGroup создаёт новую группу (doneCh отдаём "наружу", чтобы вызывающий код
// мог подписаться на отмену через select).
// Альтернативно можно было бы реализовать метод Done(), либо использовать контекст
func NewErrGroup() (*ErrGroup, chan struct{}) {

	doneCh := make(chan struct{})
	return &ErrGroup{
		doneCh: doneCh,
	}, doneCh
}

// Go запускает функцию task в отдельной горутине.
// Если канал doneCh уже закрыт (т.е. в другой горутине уже произошла ошибка),
// задача не стартует и горутина просто завершается.
func (eg *ErrGroup) Go(task func() error) {

	eg.wg.Add(1)
	go func() {
		defer eg.wg.Done()

		// быстрая проверка - если doneCh уже закрыт, немедленно выходим, не исполняя task
		select {
		case <-eg.doneCh:
			return
		default:
			if err := task(); err != nil {
				// eg.once.Do гарантирует, что только первый вызов
				// выполнит сохранение ошибки и закрытие канала
				eg.once.Do(func() {
					eg.err = err
					close(eg.doneCh)
				})
			}
		}
	}()
}

// Wait блокируется до завершения всех горутин, запущенных через Go,
// и возвращает первую возникшую ошибку, если она имеет место быть
func (eg *ErrGroup) Wait() error {

	eg.wg.Wait()
	return eg.err
}

func main() {

	// создаём группу и получаем наружу канал для отслеживания отмены
	group, groupDone := NewErrGroup()

	// запускаем 5 задач, каждая из которых ждёт случайное время от 0 до 10 секунд
	for i := 0; i < 5; i++ {
		group.Go(func() error {
			timeout := time.Second * time.Duration(rand.Intn(10))
			timer := time.NewTimer(timeout)
			defer timer.Stop()

			select {
			case <-groupDone:
				fmt.Printf("горутина %d: отменена\n", i)
				return nil // отмена - не ошибка, просто завершаемся
			case <-timer.C:
				fmt.Printf("горутина %d: timeout через %v\n", i, timeout)
				return fmt.Errorf("timeout") // в примере возвращаем одну и ту же ошибку
			}
		})
	}

	// дожидаемся завершения всех горутин и печатаем результат
	if err := group.Wait(); err != nil {
		fmt.Println(err.Error())
	}
}
