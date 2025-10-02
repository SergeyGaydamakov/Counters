# Система логирования

Проект использует настраиваемую систему логирования с различными уровнями детализации.

## Уровни логирования

- **DEBUG** - Подробная отладочная информация (все console.log перенесены на этот уровень)
- **INFO** - Общая информация о работе приложения
- **WARN** - Предупреждения о потенциальных проблемах
- **ERROR** - Ошибки и критические проблемы

## Настройка уровня логирования

### Через переменную окружения

Установите переменную окружения `LOG_LEVEL`:

```bash
# Windows
set LOG_LEVEL=DEBUG
node src/index.js

# Linux/Mac
export LOG_LEVEL=DEBUG
node src/index.js
```

### Через .env файл

Создайте файл `.env` в корне проекта:

```env
LOG_LEVEL=DEBUG
```

## Использование в коде

```javascript
const Logger = require('./utils/Logger');

// Создание логгера с уровнем по умолчанию
const logger = new Logger('DEBUG');

// Создание логгера из переменной окружения
const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Использование различных уровней
logger.debug('Отладочная информация');
logger.info('Общая информация');
logger.warn('Предупреждение');
logger.error('Ошибка');
```

## Настройка для тестов

Тесты автоматически запускаются с уровнем DEBUG:

```json
{
  "scripts": {
    "test": "LOG_LEVEL=DEBUG node src/tests/test.js",
    "test:mongo": "LOG_LEVEL=DEBUG node src/tests/mongoTest.js"
  }
}
```

## Формат вывода

Логи выводятся в следующем формате:

```
[2024-01-15T10:30:45.123Z] [DEBUG] Сообщение лога
```

Цветовая схема:
- DEBUG - голубой
- INFO - зеленый
- WARN - желтый
- ERROR - красный
