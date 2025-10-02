# Конфигурация через переменные окружения

## Создание .env файла

Создайте файл `.env` в корне проекта со следующим содержимым:

```env
# MongoDB Connection Configuration
MONGODB_CONNECTION_STRING=mongodb://127.0.0.1:27020/test

# Logging Configuration
LOG_LEVEL=DEBUG

# Test Database Configuration
TEST_DATABASE_NAME=test
```

## Доступные переменные

### MONGODB_CONNECTION_STRING
- **Описание**: Строка подключения к MongoDB
- **По умолчанию**: `mongodb://localhost:27017`
- **Примеры**:
  - `mongodb://localhost:27017/test`
  - `mongodb://username:password@localhost:27017/test`
  - `mongodb+srv://username:password@cluster.mongodb.net/test`

### LOG_LEVEL
- **Описание**: Уровень логирования
- **По умолчанию**: `INFO`
- **Возможные значения**: `DEBUG`, `INFO`, `WARN`, `ERROR`

### TEST_DATABASE_NAME
- **Описание**: Имя базы данных для тестов
- **По умолчанию**: `test`

## Использование в коде

```javascript
const EnvConfig = require('./src/utils/envConfig');

// Получить строку подключения
const connectionString = EnvConfig.getMongoConnectionString();

// Получить полную конфигурацию MongoDB
const mongoConfig = EnvConfig.getMongoConfig();

// Проверить, загружены ли переменные
if (EnvConfig.isEnvLoaded()) {
    console.log('Переменные окружения загружены');
}
```

## Запуск тестов

После создания `.env` файла тесты будут автоматически использовать настройки из переменных окружения:

```bash
npm test
```

Или отдельные тесты:

```bash
npm run test:mongo
npm run test:controller
npm run test:mongo-fact
npm run test:mongo-index
```
