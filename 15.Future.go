package main

import (
	"fmt"
	"time"
)

// Result объединяет возвращаемые значения
type Result struct {
	res any
	err error
}

// Future позволяет запустить асинхронную работу
// и получить результат вычислений позднее
type Future struct {
	result Result
	ready  chan struct{}
}

// NewFuture запускает новую работу
func NewFuture(action func() (any, error)) *Future {

	future := &Future{
		result: Result{},
		ready:  make(chan struct{}),
	}

	go func() {
		// закрываем сигнальный канал
		defer close(future.ready)

		// отправляем результат с проверкой на панику
		defer func() {
			if r := recover(); r != nil {
				future.result.err = fmt.Errorf("panic в переданной функции (LongWork): %v", r)
			}
		}()

		// получаем результат работы переданной функции
		future.result.res, future.result.err = action()
	}()

	return future
}

// GetFuture опзволяет получить результат из Future
func (f *Future) GetFuture() Result {

	<-f.ready
	return f.result
}

func LongWork(period time.Duration) (string, error) {

	time.Sleep(period)
	return "Работа выполнена.", fmt.Errorf("тут могла бы быть ошибка из LongWork")
}

func main() {

	// заворачиваем в анонимную функцию работу, результат которой
	// понадобится в будущем (посчитать надо, а ждать на месте желания нет)
	work := func() (any, error) {
		return LongWork(2 * time.Second)
	}

	// запускаем процесс получения результата
	future := NewFuture(work)

	// выполняем другую, вероятно, полезную работу
	time.Sleep(500 * time.Millisecond)

	// выполняем ещё какую-то, вероятно, не менее полезную работу
	time.Sleep(500 * time.Millisecond)

	// дожидаемся пока future отдаст результат
	result := future.GetFuture()

	fmt.Println("result.res = ", result.res)
	fmt.Println("result.err = ", result.err)
}
