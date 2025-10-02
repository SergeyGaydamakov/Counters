# Тестирование с переменными окружения

## Обзор

Все тесты теперь поддерживают конфигурацию через переменные окружения. Это позволяет легко переключаться между различными средами (локальная, тестовая, продакшн) без изменения кода.

## Быстрый старт

### 1. Создайте .env файл

Создайте файл `.env` в корне проекта:

```env
MONGODB_CONNECTION_STRING=mongodb://127.0.0.1:27020/test
LOG_LEVEL=DEBUG
TEST_DATABASE_NAME=test
```

### 2. Запустите тесты

```bash
# Все тесты
npm test

# Только тест конфигурации
npm run test:env

# Отдельные тесты
npm run test:mongo
npm run test:controller
npm run test:mongo-fact
npm run test:mongo-index
```

## Обновленные тесты

Следующие тесты теперь используют переменные окружения:

- ✅ `factControllerTest.js` - тесты FactController
- ✅ `mongoProviderTest.js` - тесты MongoProvider  
- ✅ `mongoFactTest.js` - тесты MongoFactGenerator
- ✅ `mongoFactIndexTest.js` - тесты индексных значений
- ✅ `testEnvConfig.js` - тест конфигурации окружения

## Конфигурация

### Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `MONGODB_CONNECTION_STRING` | Строка подключения к MongoDB | `mongodb://localhost:27017` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |
| `TEST_DATABASE_NAME` | Имя тестовой базы данных | `test` |

### Примеры строк подключения

```env
# Локальный MongoDB
MONGODB_CONNECTION_STRING=mongodb://localhost:27017/test

# MongoDB с аутентификацией
MONGODB_CONNECTION_STRING=mongodb://username:password@localhost:27017/test

# MongoDB Atlas
MONGODB_CONNECTION_STRING=mongodb+srv://username:password@cluster.mongodb.net/test

# MongoDB кластер (ваш случай)
MONGODB_CONNECTION_STRING=mongodb://127.0.0.1:27020/test
```

## Использование в коде

```javascript
const EnvConfig = require('./src/utils/envConfig');

// Получить строку подключения
const connectionString = EnvConfig.getMongoConnectionString();

// Получить полную конфигурацию
const config = EnvConfig.getMongoConfig();

// Создать провайдер с конфигурацией из окружения
const provider = new MongoProvider(
    config.connectionString,
    'myDatabase'
);
```

## Преимущества

1. **Гибкость**: Легко переключаться между средами
2. **Безопасность**: Чувствительные данные не попадают в код
3. **Консистентность**: Все тесты используют одинаковую конфигурацию
4. **Простота**: Один файл `.env` для всей конфигурации

## Устранение неполадок

### Тесты не подключаются к MongoDB

1. Проверьте, что MongoDB запущен
2. Убедитесь, что строка подключения корректна
3. Проверьте доступность порта

### Переменные окружения не загружаются

1. Убедитесь, что файл `.env` находится в корне проекта
2. Проверьте синтаксис файла `.env`
3. Убедитесь, что dotenv установлен: `npm install dotenv`

### Ошибки аутентификации

1. Проверьте правильность имени пользователя и пароля
2. Убедитесь, что пользователь имеет необходимые права
3. Проверьте, что база данных существует
