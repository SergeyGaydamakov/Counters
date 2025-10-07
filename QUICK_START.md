# Быстрый старт Web сервиса

## 1. Установка зависимостей

```bash
npm install
```

## 2. Настройка MongoDB

Убедитесь, что MongoDB запущен:
```bash
# Windows
mongod

# Linux/Mac
sudo systemctl start mongod
```

## 3. Запуск Web сервиса

```bash
# Запуск кластера (рекомендуется)
npm run start:web
```

Сервис будет доступен по адресу: http://localhost:3000

## 4. Тестирование API

### Health Check
```bash
curl http://localhost:3000/health
```

### Обработка события
```bash
curl -X POST http://localhost:3000/api/v1/event/purchase/json \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "productId": "prod456",
    "amount": 99.99,
    "currency": "USD"
  }'
```

### Автоматическое тестирование
```bash
npm run test:api
```

## 5. Мониторинг

- Логи выводятся в консоль
- Health check: http://localhost:3000/health
- Количество воркеров = количество CPU ядер

## 6. Остановка

Нажмите `Ctrl+C` для корректного завершения всех воркеров.

## Конфигурация

Создайте файл `.env` для настройки:

```bash
# Порт сервера
WEB_PORT=3000

# Количество воркеров (по умолчанию = количество CPU)
CLUSTER_WORKERS=4

# MongoDB
MONGODB_CONNECTION_STRING=mongodb://localhost:27017
MONGODB_DATABASE_NAME=counters

# Логирование
LOG_LEVEL=INFO
```

## Производительность

- Автоматическая кластеризация по CPU ядрам
- Rate limiting: 1000 запросов за 15 минут
- Сжатие ответов
- Оптимизированные MongoDB запросы

## Troubleshooting

1. **Порт занят**: Измените `WEB_PORT` в `.env`
2. **MongoDB не подключен**: Проверьте строку подключения
3. **Медленная работа**: Увеличьте `CLUSTER_WORKERS`
