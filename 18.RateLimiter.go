package main

import (
	"fmt"
	"sync"
	"time"
)

// RateLimiter реализует ограничитель скорости на основе алгоритма Leaky Bucket.
// "Ведро" фиксированной ёмкости наполняется токенами при запросах и равномерно "протекает"
// с заданным интервалом. Если "ведро" заполнено - запрос отклоняется.
type RateLimiter struct {
	leakyBucketCh chan struct{} // "ведро" с токенами (ёмкость = лимит)
	closeCh       chan struct{} // сигнал остановки для фоновой горутины
	doneCh        chan struct{} // сигнал, что горутина остановлена
	once          sync.Once     // гарантирует однократное закрытие closeCh
}

// NewRateLimiter создаёт лимитер с указанным пределом (limit) и периодом (rate).
// Например, limit=3, rate=1s - разрешается 3 действия в секунду с равномерным "вытеканием".
func NewRateLimiter(limit int, rate time.Duration) *RateLimiter {

	rl := &RateLimiter{
		leakyBucketCh: make(chan struct{}, limit),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	// интервал "протекания" одного токена
	interval := rate.Nanoseconds() / int64(limit)
	go rl.startPeriodicLeak(time.Duration(interval))

	return rl
}

// startPeriodicLeak фоново вынимает токены из leakyBucketCh через равные промежутки времени.
// При получении сигнала на закрытие - завершается.
func (rl *RateLimiter) startPeriodicLeak(interval time.Duration) {

	ticker := time.NewTicker(interval)

	defer func() {
		ticker.Stop()
		close(rl.doneCh) // оповещаем, что горутина завершена
	}()

	for {
		// неблокирующая проверка: если closeCh закрыт - сразу выходим
		select {
		case <-rl.closeCh:
			return
		default:
		}

		// ждём либо сигнала завершения, либо очередного тика
		select {
		case <-rl.closeCh:
			return
		case <-ticker.C:
			// вынимаем токен из "ведра" (если есть что вынимать)
			select {
			case <-rl.leakyBucketCh:
			default:
			}
		}
	}
}

// Allow пытается получить разрешение на действие.
// Если в "ведре" есть свободное место под токен, возвращает true.
// Если "ведро" заполнено или лимитер остановлен, возвращает false.
func (rl *RateLimiter) Allow() bool {

	// если лимитер уже остановлен (doneCh закрыт), сразу отказываем
	select {
	case <-rl.doneCh:
		return false
	default:
	}

	// пытаемся поместить токен в "ведро"
	select {
	case rl.leakyBucketCh <- struct{}{}:
		return true
	default:
		return false
	}
}

// Shutdown останавливает фоновую горутину и дожидается её завершения.
// После вызова Shutdown все последующие Allow() будут возвращать false.
func (rl *RateLimiter) Shutdown() {

	rl.once.Do(func() {
		close(rl.closeCh) // даём сигнал горутине остановиться
	})

	<-rl.doneCh // ждём фактического завершения горутины
}

func main() {

	// лимит 3 операции в секунду
	rl := NewRateLimiter(3, time.Second)

	// делаем 10 попыток с интервалом 100 мс
	for i := 0; i < 10; i++ {
		allowed := rl.Allow()
		fmt.Printf("Запрос %d: %v\n", i+1, allowed)
		time.Sleep(100 * time.Millisecond)
	}

	// останавливаем RateLimiter
	rl.Shutdown()

	// после остановки все вызовы отклоняются
	fmt.Println("\nПосле остановки:")
	for i := 0; i < 3; i++ {
		fmt.Printf("Запрос: %v\n", rl.Allow())
	}
}
