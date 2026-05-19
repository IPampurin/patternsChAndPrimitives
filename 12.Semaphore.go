package main

import (
	"fmt"
	"sync"
	"time"
)

// Semaphore - простая реализация семафора на основе буферизированного канала
type Semaphore struct {
	sema chan struct{} // канал-счётчик свободных слотов
}

// NewSemaphore создаёт новый семафор с заданным количеством
// разрешённых одновременных захватов (tickets > 0)
func NewSemaphore(tickets int) *Semaphore {

	if tickets <= 0 {
		fmt.Println("слотов семафора должно быть больше 0.")
		return nil
	}

	return &Semaphore{
		sema: make(chan struct{}, tickets),
	}
}

// Add захватывает один слот семафора
func (s *Semaphore) Add() {
	s.sema <- struct{}{}
}

// Rem освобождает один слот семафора
func (s *Semaphore) Rem() {
	<-s.sema
}

func main() {

	countWorkers := 5  // всего воркеров
	semaSlotCount := 3 // максимум одновременно работающих воркеров

	semaphore := NewSemaphore(semaSlotCount)
	// контролируем корректное создание экземпляра
	if semaphore == nil {
		return
	}

	var wg sync.WaitGroup

	for i := 0; i < countWorkers; i++ {

		wg.Add(1)

		go func() {
			defer wg.Done()

			semaphore.Add() // захватываем слот
			fmt.Printf("Воркер %d занял слот семафора.\n", i)

			defer func() {
				fmt.Printf("Воркер %d отдал слот семафора.\n", i)
				semaphore.Rem() // отдаём слот по выходу
			}()

			// создаём вид бурной деятельности
			fmt.Printf("Воркер %d работает!\n", i)
			time.Sleep(time.Second)
		}()
	}

	wg.Wait()
	fmt.Println("Программа завершена.")
}
