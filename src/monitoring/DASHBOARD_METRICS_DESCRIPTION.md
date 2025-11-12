# Описание для разработки дашборда метрик системы Counters

## Обзор системы метрик

Система собирает метрики с помощью Prometheus в worker'ах кластера и отправляет их мастеру каждые 10 секунд. Все метрики имеют лейблы `message_type`, `worker_id` и `endpoint` (где применимо). 

**Важно**: В дашборде `worker_id` и `endpoint` не отображаются - все метрики агрегируются по всем worker'ам и эндпоинтам для получения общей картины производительности системы.

## Структура метрик

### 1. Основные счетчики

#### `requests_total` (Counter)
- **Описание**: Общее количество запросов по типам сообщений
- **Лейблы**: `message_type`, `worker_id`, `endpoint`
- **Использование**: Мониторинг нагрузки, RPS по типам сообщений

### 2. Метрики производительности (Histogram)

#### `request_duration_seconds` 
- **Описание**: Длительность обработки HTTP запросов (мс) - общее время от получения запроса до отправки ответа, включая endpoint
- **Лейблы**: `message_type`, `worker_id`, `endpoint`
- **Buckets**: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000]
- **Использование**: P50, P95, P99 латентности HTTP запросов по эндпоинтам

#### `save_fact_duration_seconds`
- **Описание**: Длительность сохранения фактов (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000]
- **Использование**: Производительность записи в БД

#### `save_index_duration_seconds`
- **Описание**: Длительность сохранения индексов (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000]
- **Использование**: Производительность индексации

#### `counters_calculation_duration_seconds`
- **Описание**: Длительность вычисления счетчиков (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000]
- **Использование**: Производительность бизнес-логики

#### `total_processing_duration_seconds`
- **Описание**: Общая длительность обработки сообщения (мс) - время обработки факта без учета endpoint (агрегированная метрика)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000]
- **Использование**: Общая производительность обработки фактов (агрегировано по всем эндпоинтам)

### 3. Метрики объема данных

#### `total_index_count`
- **Описание**: Общее количество индексируемых полей факта (связка по полям факта и индекса)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30]
- **Использование**: Объем индексируемых данных - количество полей факта, которые используются для индексации

#### `parallel_counters_requests_count`
- **Описание**: Количество релевантных индексов (индексы, которые имеют факты после поиска по индексам)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 45, 50, 55, 60]
- **Использование**: Количество индексов, по которым найдены релевантные факты (определяет параллелизм запросов к БД)

#### `fact_counters_count`
- **Описание**: Общее количество счетчиков по всем индексам, применимых к факту (после фильтрации по условию computationConditions и применения ограничений)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500]
- **Использование**: Сложность вычислений - количество счетчиков, которые нужно вычислить для факта

#### `evaluation_counters_count`
- **Описание**: Число счетчиков, на которые влияет факт (для метрики потенциально изменяемых счетчиков)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500]
- **Использование**: Количество счетчиков, которые потенциально могут измениться при обработке факта (определяется по evaluationConditions)

#### `query_counters_count`
- **Описание**: Количество запрошенных счетчиков в запросах на вычисление счетчиков
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500]
- **Использование**: Объем запросов счетчиков - количество счетчиков, запрошенных в БД для вычисления

#### `result_counters_count`
- **Описание**: Количество полученных счетчиков, которые были вычислены
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500]
- **Использование**: Результаты вычислений счетчиков - фактическое количество счетчиков, полученных из БД после вычисления

### 4. Метрики запросов к БД

#### `relevant_facts_query_duration_seconds`
- **Описание**: Длительность запроса по индексам для поиска ИД релевантных фактов (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000]
- **Использование**: Производительность поиска релевантных фактов по индексам

#### `counters_calculation_query_duration_seconds`
- **Описание**: Длительность выполнения параллельных запросов на вычисление счетчиков по релевантным фактам (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 450, 500, 750, 1000, 1250, 1500, 2000]
- **Использование**: Производительность БД для вычисления счетчиков - время выполнения параллельных запросов к БД

### 5. Метрики Connection Pool MongoDB

#### `mongodb_connection_pool_checked_out` (Gauge)
- **Описание**: Текущее количество соединений в использовании
- **Лейблы**: `worker_id`, `client_type` ('counter' или 'aggregate')
- **Использование**: Текущая загрузка пула соединений

#### `mongodb_connection_pool_checkout_total` (Counter)
- **Описание**: Общее количество успешных операций checkout
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Статистика успешного получения соединений

#### `mongodb_connection_pool_checkin_total` (Counter)
- **Описание**: Общее количество операций checkin
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Статистика возврата соединений в пул

#### `mongodb_connection_checkout_started_total` (Counter)
- **Описание**: Общее количество попыток получить соединение
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Мониторинг запросов на соединения

#### `mongodb_connection_checkout_failed_total` (Counter)
- **Описание**: Количество неудачных попыток получить соединение
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: КРИТИЧНО - индикатор перегрузки пула

#### `mongodb_connection_created_total` (Counter)
- **Описание**: Количество созданных новых соединений
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Частота создания соединений

#### `mongodb_connection_closed_total` (Counter)
- **Описание**: Количество закрытых соединений
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Частота закрытия соединений

#### `mongodb_connection_pool_created_total` (Counter)
- **Описание**: Количество созданных пулов соединений
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Жизненный цикл пула

#### `mongodb_connection_pool_ready_total` (Counter)
- **Описание**: Количество пулов, готовых к работе
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Статус готовности пула

#### `mongodb_connection_pool_closed_total` (Counter)
- **Описание**: Количество закрытых пулов
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Статус закрытия пула

#### `mongodb_connection_pool_cleared_total` (Counter)
- **Описание**: Количество очисток пула
- **Лейблы**: `worker_id`, `client_type`
- **Использование**: Принудительная очистка пула

### 6. Метрики QueryDispatcher (Histogram)

#### `query_dispatcher_wait_time_msec`
- **Описание**: Длительность ожидания запросов в QueryDispatcher (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50, 70, 100, 150, 200]
- **Использование**: Время ожидания запросов в очереди QueryDispatcher

#### `query_dispatcher_batch_preparation_time_msec`
- **Описание**: Длительность подготовки батчей в QueryDispatcher (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50]
- **Использование**: Время подготовки батчей запросов

#### `query_dispatcher_batch_execution_time_msec`
- **Описание**: Длительность выполнения батчей в QueryDispatcher (мс) - IPC коммуникация и ожидание результатов
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 150, 200, 500, 1000]
- **Использование**: Время выполнения батчей, включая IPC коммуникацию и ожидание результатов от worker'ов

#### `query_dispatcher_results_processing_time_msec`
- **Описание**: Длительность обработки результатов в QueryDispatcher (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50]
- **Использование**: Время обработки результатов запросов

#### `query_dispatcher_results_transformation_time_msec`
- **Описание**: Длительность преобразования результатов QueryDispatcher в формат mongoProvider (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50]
- **Использование**: Время преобразования формата результатов

#### `query_dispatcher_pool_init_time_msec`
- **Описание**: Длительность инициализации пула процессов в QueryDispatcher (мс)
- **Лейблы**: `message_type`, `worker_id`
- **Buckets**: [1, 2, 3, 4, 5, 10, 15, 20, 30, 40, 50, 100, 150, 200, 500, 1000, 1500, 2000]
- **Использование**: Время инициализации пула worker-процессов

## Рекомендации по дашборду

### 1. Главная панель (Overview)
- **RPS по типам сообщений**: `sum(rate(requests_total[5m])) by (message_type)`
- **P95 латентность** (агрегировано по всем worker'ам и эндпоинтам): `histogram_quantile(0.95, sum(rate(request_duration_seconds_bucket[5m])) by (le))`
- **Средняя латентность** (агрегировано по всем worker'ам и эндпоинтам): `histogram_quantile(0.5, sum(rate(request_duration_seconds_bucket[5m])) by (le))`
- **Общий RPS**: `sum(rate(requests_total[5m]))`
- **Ошибки**: счетчик ошибок (если есть)

### 2. Панель производительности
- **Латентность по этапам**:
  - Общая обработка: `histogram_quantile(0.95, sum(rate(total_processing_duration_seconds_bucket[5m])) by (le))`
  - Сохранение фактов: `histogram_quantile(0.95, sum(rate(save_fact_duration_seconds_bucket[5m])) by (le))`
  - Сохранение индексов: `histogram_quantile(0.95, sum(rate(save_index_duration_seconds_bucket[5m])) by (le))`
  - Вычисление счетчиков: `histogram_quantile(0.95, sum(rate(counters_calculation_duration_seconds_bucket[5m])) by (le))`
- **Графики P50, P95, P99** для каждой метрики:
  - P50: `histogram_quantile(0.5, sum(rate(request_duration_seconds_bucket[5m])) by (le))`
  - P95: `histogram_quantile(0.95, sum(rate(request_duration_seconds_bucket[5m])) by (le))`
  - P99: `histogram_quantile(0.99, sum(rate(request_duration_seconds_bucket[5m])) by (le))`
- **Сравнение по типам сообщений**

### 3. Панель нагрузки
- **Общий RPS**: `sum(rate(requests_total[1m]))`
- **RPS по типам сообщений**: `sum(rate(requests_total[1m])) by (message_type)`

### 4. Панель объема данных
- **Количество индексируемых полей**: `histogram_quantile(0.95, sum(rate(total_index_count_bucket[5m])) by (le))`
- **Количество релевантных индексов**: `histogram_quantile(0.95, sum(rate(parallel_counters_requests_count_bucket[5m])) by (le))`
- **Подходящие по условию счетчики**: `histogram_quantile(0.95, sum(rate(fact_counters_count_bucket[5m])) by (le))`
- **Потенциально изменяемые счетчики**: `histogram_quantile(0.95, sum(rate(evaluation_counters_count_bucket[5m])) by (le))`
- **Запрошенные счетчики**: `histogram_quantile(0.95, sum(rate(query_counters_count_bucket[5m])) by (le))`
- **Полученные счетчики**: `histogram_quantile(0.95, sum(rate(result_counters_count_bucket[5m])) by (le))`
- **Дополнительные метрики объема**:
  - Среднее количество индексов: `histogram_quantile(0.5, sum(rate(total_index_count_bucket[5m])) by (le))`
  - Средний параллелизм: `histogram_quantile(0.5, sum(rate(parallel_counters_requests_count_bucket[5m])) by (le))`
  - Среднее число подходящих счетчиков: `histogram_quantile(0.5, sum(rate(fact_counters_count_bucket[5m])) by (le))`
  - Среднее число потенциально изменяемых счетчиков: `histogram_quantile(0.5, sum(rate(evaluation_counters_count_bucket[5m])) by (le))`
  - Среднее количество запрошенных счетчиков: `histogram_quantile(0.5, sum(rate(query_counters_count_bucket[5m])) by (le))`
  - Среднее количество полученных счетчиков: `histogram_quantile(0.5, sum(rate(result_counters_count_bucket[5m])) by (le))`

### 5. Панель БД
- **Производительность запросов фактов**: `histogram_quantile(0.95, sum(rate(relevant_facts_query_duration_seconds_bucket[5m])) by (le))`
- **Производительность запросов счетчиков**: `histogram_quantile(0.95, sum(rate(counters_calculation_query_duration_seconds_bucket[5m])) by (le))`
- **Сравнение времени БД vs обработки**
- **Дополнительные метрики БД**:
  - Средняя производительность фактов: `histogram_quantile(0.5, sum(rate(relevant_facts_query_duration_seconds_bucket[5m])) by (le))`
  - Средняя производительность счетчиков: `histogram_quantile(0.5, sum(rate(counters_calculation_query_duration_seconds_bucket[5m])) by (le))`
  - P99 производительность фактов: `histogram_quantile(0.99, sum(rate(relevant_facts_query_duration_seconds_bucket[5m])) by (le))`
  - P99 производительность счетчиков: `histogram_quantile(0.99, sum(rate(counters_calculation_query_duration_seconds_bucket[5m])) by (le))`

### 6. Панель QueryDispatcher
- **Время ожидания запросов**: `histogram_quantile(0.95, sum(rate(query_dispatcher_wait_time_msec_bucket[5m])) by (le))`
- **Время подготовки батчей**: `histogram_quantile(0.95, sum(rate(query_dispatcher_batch_preparation_time_msec_bucket[5m])) by (le))`
- **Время выполнения батчей**: `histogram_quantile(0.95, sum(rate(query_dispatcher_batch_execution_time_msec_bucket[5m])) by (le))`
- **Время обработки результатов**: `histogram_quantile(0.95, sum(rate(query_dispatcher_results_processing_time_msec_bucket[5m])) by (le))`
- **Время преобразования результатов**: `histogram_quantile(0.95, sum(rate(query_dispatcher_results_transformation_time_msec_bucket[5m])) by (le))`
- **Время инициализации пула**: `histogram_quantile(0.95, sum(rate(query_dispatcher_pool_init_time_msec_bucket[5m])) by (le))`
- **Дополнительные метрики QueryDispatcher**:
  - P50 время ожидания: `histogram_quantile(0.5, sum(rate(query_dispatcher_wait_time_msec_bucket[5m])) by (le))`
  - P50 время выполнения батчей: `histogram_quantile(0.5, sum(rate(query_dispatcher_batch_execution_time_msec_bucket[5m])) by (le))`
  - P99 время выполнения батчей: `histogram_quantile(0.99, sum(rate(query_dispatcher_batch_execution_time_msec_bucket[5m])) by (le))`

### 7. Панель Connection Pool MongoDB

#### Основные метрики использования:
- **Текущая загрузка пула** (по типам клиентов): `mongodb_connection_pool_checked_out`
- **Скорость успешных checkout**: `sum(rate(mongodb_connection_pool_checkout_total[5m])) by (client_type)`
- **Скорость checkin**: `sum(rate(mongodb_connection_pool_checkin_total[5m])) by (client_type)`
- **Неудачные попытки checkout** (КРИТИЧНО!): `sum(rate(mongodb_connection_checkout_failed_total[5m])) by (client_type)`

#### Метрики жизненного цикла:
- **Создание соединений**: `sum(rate(mongodb_connection_created_total[5m])) by (client_type)`
- **Закрытие соединений**: `sum(rate(mongodb_connection_closed_total[5m])) by (client_type)`
- **Чистое создание** (создание - закрытие): 
  ```promql
  sum(rate(mongodb_connection_created_total[5m])) by (client_type) 
  - 
  sum(rate(mongodb_connection_closed_total[5m])) by (client_type)
  ```

#### Анализ производительности:
- **Текущая загрузка по каждому worker**: `mongodb_connection_pool_checked_out by (worker_id, client_type)`
- **Агрегированная загрузка**: `sum(mongodb_connection_pool_checked_out) by (client_type)`

### 8. Алерты
- **Высокая латентность**: P95 > 1000ms
- **Низкий RPS**: < 10 запросов/мин
- **Высокая нагрузка на БД**: P95 запросов к БД > 500ms
- **Ошибки БД**: высокая латентность запросов к БД

#### Алерты Connection Pool (КРИТИЧНО):
- **Неудачные checkout** (КРИТИЧНО!): `rate(mongodb_connection_checkout_failed_total[5m]) > 0`
  - Описание: Любая попытка получить соединение из пула не удалась - пул перегружен!
  - Рекомендация: Немедленно увеличить `MONGODB_MAX_POOL_SIZE`
  
- **Высокая загрузка пула**: `mongodb_connection_pool_checked_out / mongodb_max_pool_size > 0.9`
  - Описание: 90%+ пула в использовании, возможны задержки
  - Рекомендация: Увеличить размер пула или проверить долгие операции

- **Высокий процент неудач**: процент неудачных checkout > 1%
  ```promql
  rate(mongodb_connection_checkout_failed_total[5m]) 
  / 
  rate(mongodb_connection_checkout_started_total[5m]) * 100 > 1
  ```

- **Чрезмерное создание соединений**: частое создание/закрытие указывает на проблемы с пулом
  ```promql
  rate(mongodb_connection_created_total[5m]) > 10
  AND 
  rate(mongodb_connection_closed_total[5m]) > 10
  ```

### 9. Технические детали
- **Интервал обновления**: 10 секунд (как в коде)
- **История данных**: рекомендуется 30 дней
- **Группировка**: только по `message_type` (worker_id и endpoint агрегируются)
- **Временные окна**: 1m, 5m, 15m, 1h, 24h


Этот дашборд позволит мониторить производительность системы, выявлять узкие места и оптимизировать работу кластера обработки счетчиков.
