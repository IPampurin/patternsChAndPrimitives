package main

import (
	"fmt"
	"sync"
	"time"
)

// call представляет один вызов из серии
type call struct {
	val  interface{}   // результат вызова
	err  error         // ошибка, если есть
	done chan struct{} // закрывается, когда вызов завершён (сигнал готовности)
}

// SingleFlight гарантирует, что для заданного ключа в любой момент времени
// выполняется только одна дорогая операция (action) (остальные горутины, запросившие тот же ключ,
// пока операция не завершена, ждут её результата и получают его, не запуская дублирующих вызовов)
type SingleFlight struct {
	mu    sync.Mutex
	calls map[string]*call // мапа вызовов action
}

// NewSingleFlight создаёт новый экземпляр SingleFlight
func NewSingleFlight() *SingleFlight {

	return &SingleFlight{
		calls: make(map[string]*call),
	}
}

// Do запускает функцию action для заданного ключа, если для него ещё нет активного вызова.
// Если вызов с таким ключом уже выполняется, горутина блокируется до его завершения и получает тот же результат.
func (sf *SingleFlight) Do(key string, action func() (interface{}, error)) (interface{}, error) {

	sf.mu.Lock()
	// проверяем, нет ли уже запущенного вызова для этого ключа
	if call, ok := sf.calls[key]; ok {
		// если вызов уже идёт, разблокируем мьютекс и ждём его завершения
		sf.mu.Unlock()
		return sf.wait(call)
	}

	// если для этого ключа активного вызова нет, создаём новый вызов
	call := &call{
		done: make(chan struct{}),
	}
	sf.calls[key] = call // регистрируем вызов, чтобы другие видели его
	sf.mu.Unlock()

	// запускаем action в отдельной горутине
	go func() {
		defer func() {
			// после завершения action
			sf.mu.Lock()
			close(call.done)      // закрываем канал done, чтобы разблокировать всех ожидающих
			delete(sf.calls, key) // убираем запись о вызове из мапы
			sf.mu.Unlock()
		}()

		// выполняем "дорогую" операцию и сохраняем результат
		call.val, call.err = action()
	}()

	// вызывающая горутина (которая создала вызов) тоже ждёт его завершения, чтобы вернуть результат
	return sf.wait(call)
}

// wait блокируется до закрытия канала done у вызова call, после чего возвращает его результат
func (sf *SingleFlight) wait(call *call) (interface{}, error) {

	<-call.done // ждём сигнала завершения

	return call.val, call.err
}

func main() {

	countRequests := 5

	var wg sync.WaitGroup

	sf := NewSingleFlight()

	wg.Add(countRequests)
	for i := 0; i < countRequests; i++ {

		go func() {
			defer wg.Done()

			// все горутины вызывают Do с одним и тем же ключом "same_key"
			result, err := sf.Do("same_key", func() (interface{}, error) {
				// эта функция будет выполнена только ОДИН раз,
				// остальные горутины будут ждать её завершения
				fmt.Printf("Горутина %d: single flight start\n", i)
				time.Sleep(5 * time.Second)
				fmt.Printf("Горутина %d: single flight end\n", i)
				return "Результат single flight", nil
			})

			fmt.Printf("Горутина %d: result=%v, err=%v\n", i, result, err)
		}()
	}

	wg.Wait()
}
