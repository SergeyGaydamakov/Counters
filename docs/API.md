# API Documentation

## Модули проекта

### FactGenerate

Класс для генерации случайных тестовых данных.

#### Конструктор
```javascript
new FactGenerate(fieldCount, typeCount, fieldsPerType, typeFieldsConfig)
```

**Параметры:**
- `fieldCount` (number, по умолчанию 23) - общее количество доступных полей
- `typeCount` (number, по умолчанию 5) - количество типов фактов
- `fieldsPerType` (number, по умолчанию 10) - количество полей на тип
- `typeFieldsConfig` (array, необязательно) - предопределенная конфигурация полей для типов

#### Методы

##### generateFact(type, fromDate, toDate, targetSizeBytes)
Генерирует один факт заданного типа.

**Параметры:**
- `type` (number) - тип факта (1-5)
- `fromDate` (Date) - начальная дата диапазона
- `toDate` (Date) - конечная дата диапазона
- `targetSizeBytes` (number, необязательно) - целевой размер JSON в байтах

**Возвращает:** объект факта

##### generateFacts(count, type, fromDate, toDate, targetSizeBytes)
Генерирует массив фактов одного типа.

**Параметры:**
- `count` (number) - количество фактов
- `type` (number) - тип факта
- `fromDate` (Date) - начальная дата диапазона
- `toDate` (Date) - конечная дата диапазона
- `targetSizeBytes` (number, необязательно) - целевой размер JSON в байтах

**Возвращает:** массив объектов фактов

##### generateMixedFacts(count, types, fromDate, toDate, targetSizeBytes)
Генерирует смешанные факты разных типов.

**Параметры:**
- `count` (number) - общее количество фактов
- `types` (array) - массив типов фактов для генерации
- `fromDate` (Date) - начальная дата диапазона
- `toDate` (Date) - конечная дата диапазона
- `targetSizeBytes` (number, необязательно) - целевой размер JSON в байтах

**Возвращает:** массив объектов фактов

---

### MongoFactGenerate

Класс для генерации и сохранения фактов в MongoDB.

#### Конструктор
```javascript
new MongoFactGenerate(connectionString, databaseName, collectionName)
```

**Параметры:**
- `connectionString` (string) - строка подключения к MongoDB
- `databaseName` (string) - имя базы данных
- `collectionName` (string, по умолчанию 'facts') - имя коллекции

#### Методы

##### connect()
Подключается к MongoDB.

**Возвращает:** Promise<boolean>

##### disconnect()
Отключается от MongoDB.

**Возвращает:** Promise<void>

##### generateAndInsertFacts(count, types, fromDate, toDate, batchSize, targetSizeBytes)
Генерирует и вставляет факты в MongoDB пакетами.

**Параметры:**
- `count` (number) - количество фактов
- `types` (array) - массив типов фактов
- `fromDate` (Date) - начальная дата диапазона
- `toDate` (Date) - конечная дата диапазона
- `batchSize` (number, по умолчанию 1000) - размер пакета для вставки
- `targetSizeBytes` (number, необязательно) - целевой размер JSON в байтах

**Возвращает:** Promise<object> с результатами операции

##### getFactsCount()
Получает количество документов в коллекции.

**Возвращает:** Promise<number>

##### clearFacts()
Очищает коллекцию фактов.

**Возвращает:** Promise<object>

##### getFactsStatistics()
Получает статистику по типам фактов.

**Возвращает:** Promise<void> (выводит статистику в консоль)

##### createIndexes()
Создает индексы для оптимизации производительности.

**Возвращает:** Promise<void>

##### getSampleFacts(limit, type)
Получает примеры фактов из коллекции.

**Параметры:**
- `limit` (number, по умолчанию 5) - количество примеров
- `type` (number, необязательно) - фильтр по типу

**Возвращает:** Promise<array>

---

### FactIndexer

Класс для создания индексных значений из фактов.

#### Конструктор
```javascript
new FactIndexer()
```

#### Методы

##### index(fact)
Создает массив индексных значений из одного факта.

**Параметры:**
- `fact` (object) - объект факта для индексации

**Возвращает:** Array<object> - массив индексных значений

**Структура индексного значения:**
```javascript
{
    it: Number,    // тип индекса (номер из fN)
    f: String,     // значение поля факта
    i: String,     // идентификатор факта
    t: Number,     // тип факта
    d: Date,       // дата факта
    c: Date        // дата создания
}
```

##### indexFacts(facts)
Создает индексные значения для массива фактов.

**Параметры:**
- `facts` (Array<object>) - массив фактов для индексации

**Возвращает:** Array<object> - массив всех индексных значений

---

### MongoProvider

Провайдер для работы с MongoDB коллекциями facts и factIndex с дополнительными утилитами.

#### Конструктор
```javascript
new MongoProvider(connectionString, databaseName)
```

**Параметры:**
- `connectionString` (string) - строка подключения к MongoDB
- `databaseName` (string) - имя базы данных

#### Константы
- `FACT_COLLECTION_NAME = "facts"` - имя коллекции для фактов
- `FACT_INDEX_COLLECTION_NAME = "factIndex"` - имя коллекции для индексных значений

#### Методы

##### connect() / disconnect()
Управление соединением с MongoDB.

##### createIndexes()
Создает оптимальные индексы для коллекции фактов.

##### getFactsCollectionSchema()
Анализирует и возвращает схему коллекции фактов.

**Возвращает:** Promise<object>

##### validateFactStructure(fact)
Проверяет структуру факта на соответствие требованиям.

**Параметры:**
- `fact` (object) - объект факта для проверки

**Возвращает:** object с результатами валидации

##### getCollectionStats()
Получает детальную статистику коллекции фактов.

**Возвращает:** Promise<object>

##### ensureConnection()
Проверяет и при необходимости восстанавливает соединение.

**Возвращает:** Promise<boolean>

#### Методы для работы с индексными значениями

##### createIndexValuesIndexes()
Создает составной индекс для коллекции индексных значений.

**Возвращает:** Promise<boolean>

##### insertIndexValues(indexValues)
Вставляет индексные значения в MongoDB.

**Параметры:**
- `indexValues` (Array<object>) - массив индексных значений

**Возвращает:** Promise<object> с результатами вставки

##### getIndexValuesStats()
Получает статистику коллекции индексных значений.

**Возвращает:** Promise<object>

##### getIndexValuesSchema()
Получает схему коллекции индексных значений.

**Возвращает:** Promise<object>

##### getSampleIndexValues(limit)
Получает примеры индексных значений.

**Параметры:**
- `limit` (number, по умолчанию 5) - количество примеров

**Возвращает:** Promise<Array<object>>

##### clearIndexValues()
Очищает коллекцию индексных значений.

**Возвращает:** Promise<object>

## Структура данных

### Факт
```javascript
{
    i: ObjectId,        // уникальный идентификатор (GUID преобразованный в ObjectId)
    t: Number,          // тип факта (1-5)
    a: Number,          // количество (1-1000000)
    c: Date,            // дата создания (текущая)
    d: Date,            // дата факта (из заданного диапазона)
    f1: String,         // случайные поля (10 полей из f1-f23)
    f3: String,
    // ... другие случайные поля
    z: String           // поле заполнения для достижения целевого размера (необязательное)
}
```

### Индексное значение
```javascript
{
    it: Number,         // тип индекса (номер из fN, например: f1 -> 1, f5 -> 5)
    f: String,          // значение поля факта (из fN)
    i: String,          // идентификатор факта
    t: Number,          // тип факта
    d: Date,            // дата факта
    c: Date             // дата создания JSON
}
```

### Индексы MongoDB

#### Коллекция facts
- `{i: 1}` - уникальный идентификатор
- `{t: 1}` - тип факта
- `{d: 1}` - дата факта
- `{t: 1, d: 1}` - составной индекс
- `{a: 1}` - количество

#### Коллекция factIndex
- `{f: "hashed", it: 1, d: 1, i: 1}` - составной индекс для всех основных запросов

## Примеры использования

См. файлы в каталоге `src/examples/` и `src/tests/` для подробных примеров использования всех модулей.
