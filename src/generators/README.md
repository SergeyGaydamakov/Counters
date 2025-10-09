# MongoCounters

Класс `MongoCounters` предназначен для создания счетчиков на основе конфигурации и фактов. Он анализирует факты и определяет, какие счетчики должны быть применены на основе условий в конфигурации.

## Основные возможности

- Инициализация с файлом конфигурации или массивом конфигурации
- Сопоставление условий счетчиков с данными фактов
- Поддержка MongoDB операторов (`$in`, `$nin`, `$ne`, `$not`, `$regex`, `$exists`, `$or`)
- Построение структуры для использования в MongoDB aggregate запросе с оператором `$facet`

## Использование

### Инициализация

```javascript
const { MongoCounters } = require('./src/index');

// Инициализация с файлом конфигурации
const mongoCounters = new MongoCounters('./countersConfig.json');

// Инициализация с массивом конфигурации
const config = [
    {
        name: 'payment_counter',
        comment: 'Счетчик платежей',
        condition: { messageTypeId: [50, 70] },
        aggregate: [
            { $match: { status: 'A' } },
            { $group: { _id: null, count: { $sum: 1 } } }
        ]
    }
];
const mongoCounters2 = new MongoCounters(config);
```

### Создание счетчиков для факта

```javascript
const fact = {
    _id: 'test_fact',
    t: 50,
    c: new Date(),
    d: {
        messageTypeId: 50,
        status: 'A',
        amount: 1000
    }
};

const counters = mongoCounters.make(fact);
// Результат: объект с ключами - именами счетчиков, значениями - массивами aggregate операций
```

### Вспомогательные методы

```javascript
// Получить список всех счетчиков
const names = mongoCounters.getCounterNames();

// Получить конфигурацию конкретного счетчика
const config = mongoCounters.getCounterConfig('payment_counter');

// Получить количество счетчиков
const count = mongoCounters.getCounterCount();
```

## Структура конфигурации счетчика

```javascript
{
    "name": "string",           // Имя счетчика
    "comment": "string",        // Комментарий к счетчику
    "condition": {              // Условия применения счетчика
        "field": "value",       // Простое условие
        "field": [1, 2, 3],    // Условие $in
        "field": {              // MongoDB операторы
            "$nin": ["A", "B"],
            "$ne": "C",
            "$regex": "^test",
            "$or": [...]
        }
    },
    "aggregate": [             // Массив MongoDB aggregate операций
        { "$match": {...} },
        { "$group": {...} }
    ]
}
```

## Поддерживаемые MongoDB операторы

- `$in` - значение входит в массив
- `$nin` - значение не входит в массив  
- `$ne` - значение не равно
- `$not` - отрицание условия
- `$regex` - регулярное выражение
- `$exists` - проверка существования поля
- `$or` - логическое ИЛИ

## Тестирование

Запуск тестов:
```bash
npm run test:counter
```

Запуск примера использования:
```bash
node src/examples/mongoCountersExample.js
```
