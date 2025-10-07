# Web Service для обработки событий

Высоконагруженный Web сервис для обработки событий с использованием Node.js cluster.

## Особенности

- **Кластеризация**: Автоматическое создание воркеров по количеству CPU ядер
- **Высокая производительность**: Оптимизирован для обработки большого количества запросов
- **Безопасность**: Встроенная защита от CSRF, XSS, rate limiting
- **Мониторинг**: Health check endpoints и детальное логирование
- **Graceful shutdown**: Корректное завершение работы всех воркеров

## Установка зависимостей

```bash
npm install
```

## Запуск

### Запуск кластера (рекомендуется)
```bash
npm run start:web
```

### Запуск одного воркера (для разработки)
```bash
npm run start:worker
```

## API Endpoints

### Health Check
```
GET /health
```
Возвращает статус воркера, использование памяти и время работы.

### Обработка JSON событий
```
POST /api/v1/event/{eventType}/json
```

**Параметры:**
- `eventType` (string) - тип события

**Тело запроса:**
```json
{
  "userId": "user123",
  "productId": "prod456",
  "amount": 99.99,
  "currency": "USD",
  "metadata": {
    "source": "web"
  }
}
```

**Ответ:**
```json
{
  "success": true,
  "eventType": "purchase",
  "factId": "generated-fact-id",
  "processingTime": {
    "total": 150,
    "counters": 50,
    "saveFact": 30,
    "saveIndex": 70
  },
  "counters": {},
  "timestamp": "2024-01-15T10:30:00.123Z",
  "worker": 12345
}
```

### Обработка IRIS событий (заглушка)
```
POST /api/v1/event/{eventType}/iris
```
В настоящее время не реализовано.

## Конфигурация

Настройки можно изменить через переменные окружения:

### Основные настройки
- `WEB_PORT` - порт сервера (по умолчанию: 3000)
- `CLUSTER_WORKERS` - количество воркеров (по умолчанию: количество CPU ядер)
- `LOG_LEVEL` - уровень логирования (DEBUG, INFO, WARN, ERROR)

### MongoDB настройки
- `MONGODB_CONNECTION_STRING` - строка подключения к MongoDB
- `MONGODB_DATABASE_NAME` - имя базы данных

### Настройки фактов
- `FACT_FIELD_CONFIG_PATH` - путь к конфигурации полей
- `INDEX_CONFIG_PATH` - путь к конфигурации индексов
- `FACT_TARGET_SIZE` - целевой размер факта

### Безопасность
- `CORS_ORIGIN` - разрешенные источники для CORS
- `RATE_LIMIT_MAX` - максимальное количество запросов за окно
- `RATE_LIMIT_WINDOW_MS` - окно для rate limiting в миллисекундах

### Лимиты
- `JSON_LIMIT` - максимальный размер JSON запроса
- `URL_LIMIT` - максимальный размер URL-encoded данных

## Примеры использования

### cURL
```bash
# Health check
curl -X GET http://localhost:3000/health

# Обработка события
curl -X POST http://localhost:3000/api/v1/event/purchase/json \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "productId": "prod456",
    "amount": 99.99,
    "currency": "USD"
  }'
```

### JavaScript (fetch)
```javascript
const response = await fetch('http://localhost:3000/api/v1/event/purchase/json', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    userId: 'user123',
    productId: 'prod456',
    amount: 99.99,
    currency: 'USD'
  })
});

const result = await response.json();
console.log(result);
```

## Тестирование производительности

### Apache Bench
```bash
# Базовый тест
ab -n 1000 -c 10 -H "Content-Type: application/json" -p test_data.json http://localhost:3000/api/v1/event/test/json

# Высокая нагрузка
ab -n 10000 -c 100 -H "Content-Type: application/json" -p test_data.json http://localhost:3000/api/v1/event/test/json
```

### wrk
```bash
wrk -t12 -c400 -d30s -s post.lua http://localhost:3000/api/v1/event/test/json
```

## Мониторинг

### Логи
Сервис выводит детальные логи:
- HTTP запросы с временем обработки
- Ошибки и исключения
- Статистика воркеров
- Производительность MongoDB операций

### Метрики
- Количество активных воркеров
- Использование памяти
- Время обработки запросов
- Количество обработанных событий

## Архитектура

```
┌─────────────────┐
│   Master Process │
│   (cluster.js)   │
└─────────┬───────┘
          │
    ┌─────┴─────┐
    │           │
┌───▼───┐   ┌───▼───┐
│Worker1│   │Worker2│
│(3000) │   │(3000) │
└───────┘   └───────┘
    │           │
    └─────┬─────┘
          │
    ┌─────▼─────┐
    │ MongoDB   │
    │ Database  │
    └───────────┘
```

## Troubleshooting

### Проблемы с подключением к MongoDB
1. Проверьте строку подключения
2. Убедитесь, что MongoDB запущен
3. Проверьте права доступа

### Высокое использование памяти
1. Уменьшите количество воркеров
2. Проверьте настройки MongoDB
3. Мониторьте утечки памяти

### Медленная обработка запросов
1. Проверьте производительность MongoDB
2. Увеличьте количество воркеров
3. Оптимизируйте индексы базы данных

## Разработка

### Структура файлов
```
src/web/
├── cluster.js      # Master процесс кластера
├── worker.js       # Worker процесс
├── config.js       # Конфигурация
├── routes.js       # API маршруты
├── middleware.js   # Express middleware
├── examples.js     # Примеры использования
└── README.md       # Документация
```

### Добавление новых endpoints
1. Добавьте маршрут в `routes.js`
2. Обновите документацию
3. Добавьте тесты
4. Обновите примеры в `examples.js`

# Health check
curl http://localhost:3000/health

# Обработка события
curl -X POST http://localhost:3000/api/v1/event/1/json \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "productId": "prod456",
    "amount": 99.99,
    "currency": "USD"
  }'
