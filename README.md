# MongoDB Performance Testing with FactGenerate

Проект для тестирования производительности MongoDB с использованием генерации случайных тестовых данных.

## Структура проекта

```
src/
├── index.js                    # Главный файл экспорта модулей
├── generators/                 # Классы генерации данных
│   ├── FactGenerate.js        # Класс для генерации случайных фактов
│   ├── FactIndexer.js         # Класс для создания индексов из фактов
│   └── MongoFactGenerate.js   # Класс для работы с MongoDB и массовой генерации данных
├── providers/                  # Провайдеры для работы с базами данных
│   └── MongoProvider.js       # Провайдер для работы с MongoDB
├── tests/                      # Тестовые файлы
│   ├── factTest.js            # Тесты для FactGenerate
│   ├── factIndexerTest.js     # Тесты для FactIndexer
│   ├── mongoFactTest.js       # Тесты для MongoFactGenerate
│   ├── mongoFactIndexTest.js # Тесты для работы с индексными значениями в MongoDB
│   └── loggerTest.js          # Тесты для системы логирования Logger
└── examples/                   # Примеры использования
    ├── factControllerQuickExample.js # Быстрый пример использования FactController
    ├── factIndexerExample.js   # Пример использования FactIndexer
    ├── mongoProviderExample.js # Пример использования MongoProvider
    └── mongoFactIndexExample.js # Пример работы с индексными значениями в MongoDB
```

## Установка

1. Убедитесь, что MongoDB установлен и запущен на вашей системе
2. Установите зависимости:
```bash
npm install
```

## Использование

### FactGenerate - базовая генерация фактов

```javascript
const { FactGenerate } = require('./src');
// или
const FactGenerate = require('./src/generators/FactGenerate');

const generator = new FactGenerate();
const fromDate = new Date('2024-01-01');
const toDate = new Date('2024-12-31');

// Генерация одного факта
const fact = generator.generateFact(1, fromDate, toDate);

// Генерация множественных фактов одного типа
const facts = generator.generateFacts(1000, 2, fromDate, toDate);

// Генерация смешанных типов
const mixedFacts = generator.generateMixedFacts(1000, [1, 2, 3], fromDate, toDate);
```

### FactIndexer - создание индексных значений из фактов

```javascript
const { FactIndexer } = require('./src');
// или
const FactIndexer = require('./src/generators/FactIndexer');

const indexer = new FactIndexer();

// Создание индексных значений из одного факта
const fact = {
    i: 'test-id-123',
    t: 1,
    a: 100,
    c: new Date('2024-01-01'),
    d: new Date('2024-01-15'),
    f1: 'value1',
    f2: 'value2',
    f5: 'value5'
};

const indexValues = indexer.index(fact);
// Результат: массив из 3 индексных значений со значениями полей f1, f2, f5

// Создание индексных значений из массива фактов
const facts = [fact1, fact2, fact3];
const allIndexValues = indexer.indexFacts(facts);

// Базовая информация об индексных значениях
console.log(`Всего индексных значений: ${allIndexValues.length}`);
```

### MongoFactGenerate - работа с MongoDB

```javascript
const { MongoFactGenerate } = require('./src');
// или
const MongoFactGenerate = require('./src/generators/MongoFactGenerate');

const mongoGen = new MongoFactGenerate(
    'mongodb://localhost:27017',  // строка подключения
    'testDB',                     // имя базы данных
    'facts'                       // имя коллекции
);

async function example() {
    // Подключение
    await mongoGen.connect();
    
    // Генерация и вставка 10000 фактов
    const result = await mongoGen.generateAndInsertFacts(
        10000,                           // количество
        [1, 2, 3, 4, 5],                // типы фактов
        new Date('2024-01-01'),         // начальная дата
        new Date('2024-12-31'),         // конечная дата
        1000,                           // размер пакета
        800                             // целевой размер JSON документа
    );
    
    // Получение статистики
    await mongoGen.getFactsStatistics();
    
    // Закрытие соединения
    await mongoGen.disconnect();
}
```

## Запуск тестов

### Использование npm скриптов (рекомендуется)
```bash
npm test                # Тесты FactGenerate
npm run test:mongo      # Тесты MongoDB (требует запущенный MongoDB)
npm run test:logger     # Тесты системы логирования Logger
npm run example:mongo   # Пример использования MongoProvider
npm run example:controller # Быстрый пример FactController
```

### Прямой запуск
```bash
node src/tests/factTest.js                # Тесты FactGenerate
node src/tests/factIndexerTest.js         # Тесты FactIndexer
node src/tests/mongoFactTest.js           # Тесты MongoDB
node src/tests/mongoFactIndexTest.js    # Тесты индексных значений в MongoDB
node src/tests/loggerTest.js              # Тесты системы логирования Logger
node src/examples/factControllerQuickExample.js # Быстрый пример FactController
node src/examples/factIndexerExample.js   # Пример FactIndexer
node src/examples/mongoProviderExample.js # Пример MongoProvider
node src/examples/mongoFactIndexExample.js # Пример индексных значений в MongoDB
```

## Структура данных

Каждый факт содержит:
- `i` (GUID/ObjectId) - уникальный идентификатор в формате GUID, сохраняется как ObjectId в MongoDB
- `t` (byte) - тип факта (1-5)
- `a` (double) - количество (1-1000000)
- `c` (datetime) - дата и время создания объекта (текущая дата)
- `d` (datetime) - дата факта (случайная дата в заданном диапазоне)
- 10 случайных полей `f1`-`f23` со случайными строками (2-20 символов)
- `z` (string) - поле заполнения для достижения заданного размера JSON (необязательное)

## Структура индексных значений

Каждое индексное значение содержит:
- `it` (number) - тип индекса (номер из названия поля fN, где N - число)
- `f` (string) - значение поля факта, которое начинается с 'f' и содержит число (f1, f2, f5, f10, etc.)
- `i` (string) - идентификатор факта из исходного факта
- `t` (number) - тип факта из исходного факта
- `d` (Date) - дата и время факта из исходного факта
- `c` (Date) - дата и время создания JSON из исходного факта

**Примечание:** Если в факте нет полей, начинающихся с 'f', возвращается пустой массив индексных значений.

## Возможности FactIndexer

### Основные методы:
- `index(fact)` - создание индексных значений из одного факта
- `indexFacts(facts)` - создание индексных значений из массива фактов

### Особенности:
- Автоматическое обнаружение полей, начинающихся с 'f' и содержащих число
- Обработка фактов без полей fN (возвращается пустой массив)
- Валидация входных данных
- Обработка ошибок при создании индексных значений из массива фактов

## Возможности MongoFactGenerate

### Основные методы:
- `connect()` - подключение к MongoDB
- `disconnect()` - отключение от MongoDB
- `generateAndInsertFacts()` - генерация и вставка фактов
- `getFactsCount()` - подсчет документов в коллекции
- `clearFacts()` - очистка коллекции
- `getFactsStatistics()` - статистика по типам фактов
- `createIndexes()` - создание индексов для оптимизации
- `getSampleFacts()` - получение примеров фактов

### Особенности:
- Пакетная вставка для оптимизации производительности
- Автоматическое преобразование GUID в ObjectId при сохранении в MongoDB
- Поддержка контроля размера JSON документов через поле `z`
- Обработка ошибок и отчеты о прогрессе
- Автоматическое создание индексов (включая индекс на поле `i`)
- Статистика производительности (скорость вставки)
- Поддержка различных конфигураций типов фактов

## Конфигурация MongoDB

По умолчанию используется:
- Хост: `localhost:27017`
- База данных: `factTestDB`
- Коллекция: `facts`

Измените параметры в `src/tests/mongoFactTest.js` для использования других настроек.

### MongoProvider - дополнительные возможности

```javascript
const { MongoProvider } = require('./src');
// или
const MongoProvider = require('./src/providers/MongoProvider');

const provider = new MongoProvider(
    'mongodb://localhost:27017',
    'testDB'
);

async function example() {
    await provider.connect();
    
    // Создание индексов
    await provider.createIndexes();
    
    // Получение схемы коллекции
    const schema = await provider.getFactsCollectionSchema();
    
    // Валидация данных
    const isValid = provider.validateFactStructure(factData);
    
    await provider.disconnect();
}
```

### Работа с индексными значениями в MongoDB

```javascript
const { MongoProvider, FactIndexer, FactGenerate } = require('./src');

const provider = new MongoProvider(
    'mongodb://localhost:27017',
    'testDB'
);
const indexer = new FactIndexer();
const generator = new FactGenerate();

async function indexValuesExample() {
    await provider.connect();
    
    // Генерация фактов и создание индексных значений
    const facts = generator.generateFacts(100, 1, fromDate, toDate);
    const indexValues = indexer.indexFacts(facts);
    
    // Вставка индексных значений в MongoDB
    await provider.insertIndexValues(indexValues);
    
    // Создание составного индекса (f: hashed, it: 1, d: 1, i: 1)
    await provider.createIndexValuesIndexes();
    
    // Получение статистики
    const stats = await provider.getIndexValuesStats();
    
    // Получение схемы коллекции
    const schema = await provider.getIndexValuesSchema();
    
    // Получение примеров
    const samples = await provider.getSampleIndexValues(5);
    
    await provider.disconnect();
}
```

### Константы MongoProvider:
- `FACT_COLLECTION_NAME = "facts"` - имя коллекции для фактов
- `FACT_INDEX_COLLECTION_NAME = "factIndex"` - имя коллекции для индексных значений

### Основные методы MongoProvider:
- `connect()` / `disconnect()` - управление соединением
- `createIndexes()` - создание оптимальных индексов
- `getFactsCollectionSchema()` - анализ схемы коллекции фактов
- `validateFactStructure()` - валидация структуры фактов
- `getCollectionStats()` - статистика коллекции
- `ensureConnection()` - проверка и восстановление соединения

### Работа с индексными значениями в MongoDB:
- `createIndexValuesIndexes()` - создание составного индекса для коллекции индексных значений
- `insertIndexValues()` - вставка индексных значений в MongoDB
- `getIndexValuesStats()` - статистика коллекции индексных значений
- `getIndexValuesSchema()` - схема коллекции индексных значений
- `getSampleIndexValues()` - получение примеров индексных значений
- `clearIndexValues()` - очистка коллекции индексных значений

## Производительность

Типичная производительность на локальной машине:
- 1000-5000 фактов/сек при пакетной вставке
- Оптимальный размер пакета: 500-1000 документов
- Индексы значительно ускоряют запросы на выборку

### Индексы для индексных значений:
- **Составной индекс `{f: "hashed", it: 1, d: 1, i: 1}`** - оптимизирует все основные запросы:
  - Поиск по значению поля `f` (хешированный)
  - Фильтрация по типу поля `it` (f1, f2, f5, etc.)
  - Фильтрация по дате факта `d`
  - Связь с исходными фактами через `i`

## Устранение неполадок

1. **MongoDB не запущен**: Убедитесь, что MongoDB сервер запущен
2. **Ошибка подключения**: Проверьте строку подключения и доступность порта
3. **Нехватка памяти**: Уменьшите размер пакета или общее количество фактов
4. **Медленная вставка**: Проверьте индексы и размер пакета

## Дополнительная документация

- [📖 API Documentation](docs/API.md) - Подробное описание всех методов и классов
- [🛠️ Development Guide](docs/DEVELOPMENT.md) - Руководство по разработке и расширению проекта
- [📋 Technical Requirements](Prompt.md) - Техническое задание проекта
- [🔧 Logging System](docs/LOGGING.md) - Документация по системе логирования
- [🧪 Logger Testing](docs/LOGGER_TESTING.md) - Тестирование системы логирования

## Быстрый старт

1. **Установка зависимостей:**
   ```bash
   npm install
   ```

2. **Запуск базовых тестов:**
   ```bash
   npm test
   ```

3. **Тестирование системы логирования:**
   ```bash
   npm run test:logger
   ```

4. **Тестирование FactIndexer (создание индексных значений):**
   ```bash
   node src/tests/factIndexerTest.js
   ```

5. **Тестирование с MongoDB** (требует запущенный MongoDB):
   ```bash
   npm run test:mongo
   ```

6. **Просмотр примеров:**
   ```bash
   npm run example:controller                   # Быстрый пример FactController
   node src/examples/factIndexerExample.js      # Пример FactIndexer (индексные значения)
   node src/examples/mongoFactIndexExample.js # Пример индексных значений в MongoDB
   npm run example:mongo                        # Пример MongoProvider
   ```
