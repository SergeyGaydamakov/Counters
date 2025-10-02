# Руководство по разработке

## Структура проекта

Проект организован по принципу модульной архитектуры:

### 📁 src/
Основной каталог с исходным кодом

#### 📁 generators/
Классы для генерации данных:
- `FactGenerate.js` - базовый генератор случайных фактов
- `MongoFactGenerate.js` - генератор с интеграцией MongoDB
- `FactIndexer.js` - создание индексных значений из фактов

#### 📁 providers/
Провайдеры для работы с базами данных:
- `MongoProvider.js` - утилиты для работы с MongoDB (facts и factIndex коллекции)

#### 📁 tests/
Тестовые файлы:
- `factTest.js` - тесты базового функционала FactGenerate
- `mongoFactTest.js` - тесты MongoDB интеграции
- `factIndexerTest.js` - тесты FactIndexer
- `mongoIndexValuesTest.js` - тесты работы с индексными значениями в MongoDB

#### 📁 examples/
Примеры использования:
- `mongoProviderExample.js` - демонстрация возможностей MongoProvider
- `factIndexerExample.js` - пример использования FactIndexer
- `mongoFactIndexExample.js` - пример работы с индексными значениями в MongoDB

#### 📄 index.js
Главный файл экспорта модулей проекта

## Принципы организации кода

### 1. Модульность
Каждый класс находится в отдельном файле и выполняет одну основную функцию.

### 2. Разделение ответственности
- **Generators** - генерация данных и создание индексных значений
- **Providers** - работа с базами данных (MongoDB)
- **Tests** - тестирование функциональности
- **Examples** - демонстрация использования

### 3. Консистентность импортов
```javascript
// Использование через главный экспорт (рекомендуется)
const { FactGenerate, MongoFactGenerate, FactIndexer, MongoProvider } = require('./src');

// Прямой импорт модулей
const FactGenerate = require('./src/generators/FactGenerate');
const FactIndexer = require('./src/generators/FactIndexer');
const MongoProvider = require('./src/providers/MongoProvider');
```

## Добавление новых модулей

### Новый генератор данных
1. Создайте файл в `src/generators/`
2. Добавьте экспорт в `src/index.js`
3. Создайте тесты в `src/tests/`
4. Добавьте пример в `src/examples/`

### Новый провайдер базы данных
1. Создайте файл в `src/providers/`
2. Следуйте интерфейсу MongoProvider
3. Добавьте экспорт в `src/index.js`
4. Создайте соответствующие тесты

## Стандарты кодирования

### Именование файлов
- PascalCase для классов: `FactGenerate.js`
- camelCase для утилит: `mongoProviderExample.js`
- kebab-case для документации: `test-report.md`

### Структура классов
```javascript
class ExampleClass {
    constructor(params) {
        // Инициализация
    }

    /**
     * Документация метода
     * @param {type} param - описание
     * @returns {type} описание возвращаемого значения
     */
    methodName(param) {
        // Реализация
    }
}

module.exports = ExampleClass;
```

### Обработка ошибок
```javascript
async function example() {
    try {
        // Основная логика
    } catch (error) {
        console.error('Описание ошибки:', error.message);
        throw error; // При необходимости
    }
}
```

## Тестирование

### Запуск тестов
```bash
npm test                # Базовые тесты
npm run test:mongo      # MongoDB тесты (требует запущенный MongoDB)
npm run example:mongo   # Демонстрационные примеры

# Прямой запуск тестов
node src/tests/factTest.js                # Тесты FactGenerate
node src/tests/factIndexerTest.js         # Тесты FactIndexer
node src/tests/mongoFactTest.js           # Тесты MongoDB
node src/tests/mongoIndexValuesTest.js    # Тесты индексных значений
```

### Добавление новых тестов
1. Создайте файл в `src/tests/`
2. Добавьте npm script в `package.json`
3. Следуйте существующей структуре тестов

## MongoDB настройки

### Локальная разработка
```javascript
const config = {
    connectionString: 'mongodb://localhost:27017',
    databaseName: 'factTestDB'
    // Имена коллекций задаются константами в MongoProvider:
    // FACT_COLLECTION_NAME = "facts"
    // FACT_INDEX_COLLECTION_NAME = "factIndex"
};
```

### Рекомендуемые индексы

#### Коллекция facts
```javascript
// Создаются автоматически через MongoProvider.createIndexes()
db.facts.createIndex({ "i": 1 });  // уникальный идентификатор
db.facts.createIndex({ "t": 1 });  // тип факта
db.facts.createIndex({ "d": 1 });  // дата факта
db.facts.createIndex({ "t": 1, "d": 1 }); // составной индекс
db.facts.createIndex({ "a": 1 });  // количество
```

#### Коллекция factIndex
```javascript
// Создается автоматически через MongoProvider.createIndexValuesIndexes()
db.factIndex.createIndex({ 
    "f": "hashed", 
    "it": 1, 
    "d": 1, 
    "i": 1 
}); // составной индекс для всех основных запросов
```

## Работа с индексными значениями

### Создание индексных значений
```javascript
const { FactIndexer, FactGenerate } = require('./src');

const indexer = new FactIndexer();
const generator = new FactGenerate();

// Генерация фактов
const facts = generator.generateFacts(10, 1, fromDate, toDate);

// Создание индексных значений
const indexValues = indexer.indexFacts(facts);
```

### Сохранение в MongoDB
```javascript
const { MongoProvider } = require('./src');

const provider = new MongoProvider(connectionString, databaseName);
await provider.connect();

// Вставка индексных значений
await provider.insertIndexValues(indexValues);

// Создание индексов
await provider.createIndexValuesIndexes();
```

### Структура индексных значений
- `it` - тип индекса (номер из fN)
- `f` - значение поля факта
- `i` - идентификатор факта
- `t` - тип факта
- `d` - дата факта
- `c` - дата создания

## Производительность

### Рекомендации по оптимизации
1. **Размер пакета**: 500-1000 документов для вставки
2. **Индексы**: создавайте до массовой вставки данных
3. **Размер документов**: контролируйте через параметр `targetSizeBytes`
4. **Соединение**: переиспользуйте соединения MongoDB
5. **Индексные значения**: создавайте после вставки фактов

### Мониторинг
- Используйте встроенную статистику производительности
- Мониторьте память при больших объемах данных
- Контролируйте размер коллекций (facts и factIndex)
- Отслеживайте производительность составных индексов

## Развертывание

### Подготовка к продакшену
1. Настройте правильные строки подключения
2. Установите подходящие размеры пакетов
3. Создайте необходимые индексы
4. Настройте мониторинг производительности

### Переменные окружения
```bash
MONGO_CONNECTION_STRING=mongodb://localhost:27017
MONGO_DATABASE_NAME=factTestDB
# Имена коллекций задаются константами в коде:
# FACT_COLLECTION_NAME=facts
# FACT_INDEX_COLLECTION_NAME=factIndex
```
