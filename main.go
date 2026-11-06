package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Шаг 1: Подготовка структур данных

// Job представляет задание на обработку конкретного URL.
type Job struct {
	ID  int
	URL string
}

// Result содержит информацию о результатах выполнения задания.
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

// Шаг 2: Создайте функцию-воркер
// worker последовательно читает задачи из канала jobs, имитирует HTTP-запрос
// и отправляет результаты в канал results.
func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done() // Сообщаем о завершении работы воркера перед выходом

	for job := range jobs {
		// Имитируем отправку HTTP-запроса со случайной задержкой от 100 до 600 мс
		randomMillis := rand.Intn(500) + 100
		duration := time.Duration(randomMillis) * time.Millisecond
		time.Sleep(duration)

		// Формируем результат выполнения задачи
		status := "Success"
		if randomMillis%7 == 0 { // Имитируем редкую ошибку для реалистичности отчета
			status = "Failed"
		}

		results <- Result{
			Job:      job,
			Status:   status,
			Duration: duration,
		}
	}
}

func main() {
	// Инициализируем генератор случайных чисел
	rand.Seed(time.Now().UnixNano())

	// Исходный список URL-адресов для анализа
	urls := []string{
		"https://google.com",
		"https://github.com",
		"https://golang.org",
		"https://yandex.ru",
		"https://habr.com",
		"https://stackoverflow.com",
		"https://reddit.com",
		"https://medium.com",
		"https://youtube.com",
		"https://wikipedia.org",
	}

	const numWorkers = 5 // Ограничение пула воркеров (Worker Pool)

	// Шаг 3: Реализуйте паттерн Fan-out / Worker Pool
	// Создаем каналы. Буферизируем их по количеству задач, чтобы избежать блокировок.
	jobs := make(chan Job, len(urls))
	results := make(chan Result, len(urls))

	var wg sync.WaitGroup

	// Запускаем фиксированное количество горутин-воркеров (Fan-out)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// Шаг 5: Генерация заданий
	// Наполняем канал jobs исходными данными
	for i, url := range urls {
		jobs <- Job{ID: i + 1, URL: url}
	}
	close(jobs) // Закрываем канал заданий: воркеры поймут, что работа закончена

	// Шаг 4: Реализуйте паттерн Fan-in (сбор результатов)
	// В отдельной горутине дожидаемся выполнения всех воркеров и закрываем результаты.
	go func() {
		wg.Wait()
		close(results) // Закрытие канала сигнализирует циклу в main о завершении сбора
	}()

	// Шаг 5: Агрегация результатов в главной горутине
	var allResults []Result
	for res := range results {
		allResults = append(allResults, res)
	}

	// Шаг 5: Вывод финального отчета и статистики
	fmt.Println("======= ФИНАЛЬНЫЙ ОТЧЕТ ОБРАБОТКИ URL =======")
	fmt.Printf("%-3s | %-26s | %-8s | %-10s\n", "ID", "URL", "Статус", "Длительность")
	fmt.Println("-------------------------------------------------------")

	var totalDuration time.Duration
	successCount := 0

	for _, res := range allResults {
		fmt.Printf("%-3d | %-26s | %-8s | %-10s\n",
			res.Job.ID, res.Job.URL, res.Status, res.Duration)

		totalDuration += res.Duration
		if res.Status == "Success" {
			successCount++
		}
	}

	// Расчет общей статистики
	var avgDuration time.Duration
	if len(allResults) > 0 {
		avgDuration = totalDuration / time.Duration(len(allResults))
	}

	fmt.Println("-------------------------------------------------------")
	fmt.Println("======= ОБЩАЯ СТАТИСТИКА =======")
	fmt.Printf("Всего URL обработано:    %d\n", len(allResults))
	fmt.Printf("Успешных операций:       %d\n", successCount)
	fmt.Printf("Ошибочных операций:      %d\n", len(allResults)-successCount)
	fmt.Printf("Среднее время запроса:   %v\n", avgDuration)
	fmt.Printf("Общее время выполнения:  %v\n", totalDuration)
}
