package main

import (
	"fmt"
	"sync"
)

// Barrier реализует двухфазный циклический барьер для синхронизации горутин.
// Горутины, вызвавшие Before(), блокируются, пока все участники не соберутся у барьера.
// Затем они вместе выполняют первую фазу работы, после чего вызывают After() и снова
// синхронизируются перед второй фазой. Барьер можно использовать многократно (циклически).
type Barrier struct {
	size     int           // количество участников барьера
	count    int           // счётчик горутин, достигших текущей фазы
	beforeCh chan struct{} // канал для синхронизации перед первой фазой
	afterCh  chan struct{} // канал для синхронизации перед второй фазой
	mu       sync.Mutex    // мьютекс для безопасного обновления счётчика
}

// NewBarrier создаёт новый барьер на size участников.
// Если size <= 0, функция паникует, так как барьер без участников не имеет смысла.
func NewBarrier(size int) *Barrier {

	if size <= 0 {
		panic("size должен быть больше 0")
	}

	return &Barrier{
		size:     size,
		count:    0,
		beforeCh: make(chan struct{}, size),
		afterCh:  make(chan struct{}, size),
	}
}

// Before вызывается горутиной, когда она готова выполнить первую фазу работы.
// Горутина блокируется до тех пор, пока все участники не вызовут Before().
// Когда последняя горутина вызывает Before(), она разблокирует всех.
func (b *Barrier) Before() {

	b.mu.Lock()

	b.count++ // увеличиваем счётчик прибывших горутин
	// если мы последние - заполняем канал токенами, чтобы разблокировать всех
	if b.count == b.size {
		for i := 0; i < b.size; i++ {
			b.beforeCh <- struct{}{}
		}
	}

	b.mu.Unlock()

	// ждём, пока в канале появится токен (т.е. пока все соберутся)
	<-b.beforeCh
}

// After вызывается горутиной после завершения первой фазы работы.
// Аналогично Before(), горутина блокируется, пока все не вызовут After().
func (b *Barrier) After() {

	b.mu.Lock()

	b.count-- // уменьшаем счётчик (горутина выбывает из первой фазы)
	// если мы последние - заполняем канал токенами для разблокировки всех
	if b.count == 0 {
		for i := 0; i < b.size; i++ {
			b.afterCh <- struct{}{}
		}
	}

	b.mu.Unlock()

	// ждём разблокировки
	<-b.afterCh
}

func main() {

	workOne := func() {
		fmt.Println("work №1")
	}
	workTwo := func() {
		fmt.Println("    work №2")
	}

	periodicity := 3 // количество горутин и одновременно количество итераций для каждой
	barrier := NewBarrier(periodicity)

	var wg sync.WaitGroup

	for i := 0; i < periodicity; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < periodicity; j++ {
				// синхронизируемся перед первой фазой
				barrier.Before()
				workOne()
				// синхронизируемся перед второй фазой
				barrier.After()
				workTwo()
			}
		}()
	}

	wg.Wait()
	fmt.Println("\nПрограмма завершена.")
}
