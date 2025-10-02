# FactController

`FactController` - это класс-контроллер для управления фактами и их индексными значениями. Он обеспечивает создание фактов с автоматической генерацией и сохранением индексных значений через уже подключенный dbProvider.

## Основные возможности

- **Создание фактов**: Создание отдельных фактов или пакетов фактов
- **Автоматическая индексация**: Автоматическое создание индексных значений из полей f1, f2, f3... факта
- **Bulk операции**: Эффективное сохранение фактов и индексов через bulk операции провайдера
- **Обработка дубликатов**: Автоматическое обновление существующих фактов при совпадении ID
- **Валидация данных**: Проверка обязательных полей и структуры фактов
- **Гибкость провайдеров**: Работа с любым dbProvider данных, реализующим необходимый интерфейс

## Установка и подключение

```javascript
const FactController = require('./controllers/FactController');
const MongoProvider = require('./providers/MongoProvider');

// Создание и подключение провайдера данных
const mongoProvider = new MongoProvider(connectionString, databaseName);
await mongoProvider.connect();

// Создание экземпляра контроллера с dbProvider
const factController = new FactController(mongoProvider);
```

## Требования к dbProvider

dbProvider должен реализовывать следующий интерфейс:

```javascript
class dbProvider {
    // Bulk вставка факта и индексных значений
    async bulkInsert(fact, factIndexes, options) {
        // Возвращает Promise<Object> с результатом операции
        // Должен содержать поля: success, factId, factInserted, factUpdated, 
        // indexesInserted, indexesUpdated, indexErrors, error
    }


}
```

## Основные методы

### createFact(fact, options)

Создает один факт и связанные индексные значения.

**Параметры:**
- `fact` (Object) - объект факта для создания
- `options` (Object, опционально) - опции операции
  - `factMatchField` (string) - поле для поиска существующего факта (по умолчанию 'i')
  - `indexMatchField` (string) - поле для поиска существующих индексных значений (по умолчанию 'i')

**Возвращает:**
```javascript
{
    success: boolean,           // успешность операции
    factId: ObjectId,          // ID созданного факта
    factInserted: number,      // количество вставленных фактов (0 или 1)
    factUpdated: number,       // количество обновленных фактов (0 или 1)
    indexesCreated: number,    // количество созданных индексных значений
    indexesInserted: number,   // количество вставленных индексных значений
    indexesUpdated: number,    // количество обновленных индексных значений
    indexErrors: Array,        // ошибки при сохранении индексов
    error: string,             // ошибка операции (если есть)
    factIndexes: Array         // созданные индексные значения
}
```

**Пример:**
```javascript
const fact = {
    i: '550e8400-e29b-41d4-a716-446655440001', // GUID
    t: 1,                                       // тип факта
    a: 100,                                     // количество
    c: new Date(),                              // дата создания
    d: new Date(),                              // дата факта
    f1: 'значение1',                           // поле для индексации
    f2: 'значение2',                           // поле для индексации
    f5: 'значение5'                            // поле для индексации
};

const result = await factController.createFact(fact);
console.log('Создано индексных значений:', result.indexesCreated);
```

### createFacts(facts, options)

Создает несколько фактов и их индексные значения.

**Параметры:**
- `facts` (Array) - массив фактов для создания
- `options` (Object, опционально) - опции операции

**Возвращает:**
```javascript
{
    success: boolean,              // общая успешность операции
    totalFacts: number,            // общее количество фактов
    successfulFacts: number,       // количество успешно обработанных фактов
    failedFacts: number,           // количество неудачных фактов
    totalIndexesCreated: number,   // общее количество созданных индексных значений
    totalIndexesInserted: number,  // общее количество вставленных индексных значений
    totalIndexesUpdated: number,   // общее количество обновленных индексных значений
    errors: Array,                 // массив ошибок
    factResults: Array             // результаты для каждого факта
}
```


## Структура факта

Факт должен содержать следующие обязательные поля:

```javascript
{
    i: string,        // уникальный идентификатор (GUID)
    t: number,        // тип факта (1-100)
    a: number,        // количество (1-1000000)
    c: Date,          // дата создания объекта
    d: Date,          // дата факта
    f1: string,       // опциональное поле для индексации
    f2: string,       // опциональное поле для индексации
    f3: string,       // опциональное поле для индексации
    // ... fN - любое количество полей f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23
    z: string         // опциональное поле заполнения
}
```

## Индексные значения

Для каждого поля fN (f1, f2, f3...) автоматически создается индексное значение:

```javascript
{
    it: number,       // номер поля (1, 2, 3...)
    f: string,        // значение поля fN
    i: ObjectId,      // ID факта
    t: number,        // тип факта
    d: Date,          // дата факта
    c: Date           // дата создания
}
```

## Примеры использования

### Базовый пример

```javascript
const FactController = require('./controllers/FactController');
const MongoProvider = require('./providers/MongoProvider');

async function example() {
    // Создаем и подключаем провайдер данных
    const mongoProvider = new MongoProvider('mongodb://localhost:27017', 'CounterTest');
    await mongoProvider.connect();
    
    // Создаем контроллер с провайдером
    const factController = new FactController(mongoProvider);
    
    try {
        const fact = {
            i: 'test-fact-001',
            t: 1,
            a: 100,
            c: new Date(),
            d: new Date(),
            f1: 'значение1',
            f2: 'значение2'
        };
        
        const result = await factController.createFact(fact);
        console.log('Результат:', result);
        
    } finally {
        await mongoProvider.disconnect();
    }
}
```

### Создание нескольких фактов

```javascript
const facts = [
    {
        i: 'fact-001',
        t: 1,
        a: 100,
        c: new Date(),
        d: new Date(),
        f1: 'value1',
        f3: 'value3'
    },
    {
        i: 'fact-002',
        t: 2,
        a: 200,
        c: new Date(),
        d: new Date(),
        f2: 'value2',
        f4: 'value4'
    }
];

const result = await factController.createFacts(facts);
console.log(`Создано ${result.successfulFacts} из ${result.totalFacts} фактов`);
```


## Обработка ошибок

Все методы возвращают объект с полем `success` и детальной информацией об ошибках:

```javascript
const result = await factController.createFact(invalidFact);

if (!result.success) {
    console.error('Ошибка:', result.error);
    console.error('Ошибки индексов:', result.indexErrors);
}
```

## Создание собственного dbProvider

Вы можете создать собственный dbProvider для работы с любым источником данных:

```javascript
class CustomProvider {
    constructor(connectionConfig) {
        this.config = connectionConfig;
    }

    // Обязательный метод
    async bulkInsert(fact, factIndexes, options) {
        // Ваша логика сохранения факта и индексных значений
        // Должна возвращать объект с полями:
        // success, factId, factInserted, factUpdated, 
        // indexesInserted, indexesUpdated, indexErrors, error
        
        return {
            success: true,
            factId: 'generated-id',
            factInserted: 1,
            factUpdated: 0,
            indexesInserted: factIndexes.length,
            indexesUpdated: 0,
            indexErrors: [],
            error: null
        };
    }

}

// Использование с собственным dbProvider
const customProvider = new CustomProvider({ host: 'localhost', port: 5432 });
const factController = new FactController(customProvider);
```

## Зависимости

- `FactIndexer` - для создания индексных значений
- dbProvider (например, `MongoProvider`) - для работы с источником данных

## Тестирование

Запуск тестов:
```bash
node src/tests/factControllerTest.js
```

Запуск примера:
```bash
node src/examples/factControllerExample.js
```
