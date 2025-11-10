# CSV to Counters Config Parser

Парсер для преобразования файла `counters.csv` в `countersCfg.json` согласно требованиям MongoDB aggregation pipeline.

## Описание

Парсер преобразует CSV файл с описанием счетчиков в JSON структуру, которая может использоваться для MongoDB запросов. Поддерживает все необходимые операторы и условия.

## Использование

```javascript
const CountersCsvParser = require('./csvToCountersCfgParser');

const parser = new CountersCsvParser();
parser.parseCsvToJson('input.csv', 'output.json');
```

Или запуск напрямую:

```bash
node src/parsers/csvToCountersCfgParser.js
```

## Поддерживаемые операторы

### Операторы сравнения
- `=` - равно (поддерживает множественные значения)
- `≠` - не равно (поддерживает множественные значения)
- `>` - больше
- `<` - меньше
- `≥` - больше или равно
- `≤` - меньше или равно

### Операторы строк
- `=*=` - содержит подстроку
- `¬=*=` - не содержит подстроку
- `*=` - начинается с подстроки
- `¬*=` - не начинается с подстроки
- `≈` - приблизительно равно (regex с игнорированием регистра)
- `¬≈` - не приблизительно равно

### Логические операторы
- `is true` - поле равно true
- `is false` - поле равно false

### Арифметика с датами
- `fieldName > ([otherField] + 720d)` - поле больше даты + количество дней
- `fieldName < ([otherField] - 30d)` - поле меньше даты - количество дней
- Поддерживаемые единицы: d (дни), h (часы), m (минуты), s (секунды)

### Ссылки на поля
- `{fieldName}` - ссылка на другое поле (computationConditions)
- `$$fieldName` - ссылка на другое поле (evaluationConditions)
- `[fieldName]` - сравнение полей через $expr

## Поддерживаемые атрибуты

- `frequency` - частота (`$sum: 1`)
- `total amount` - общая сумма (`$sum: "$d.Amount"`)
- `average amount` - средняя сумма (`$avg: "$d.Amount"`)
- `maximum amount` - максимальная сумма (`$max: "$d.Amount"`)
- `minimum amount` - минимальная сумма (`$min: "$d.Amount"`)
- `distinct values number` - количество уникальных значений (`$addToSet: "$d.PAN"`)
- `most occurrence ratio` - комбинация frequency и distinct values

## Структура выходного JSON

```json
{
  "counters": [
    {
      "name": "counter_name",
      "comment": "Описание счетчика",
      "indexTypeName": "idx_name",
      "computationConditions": {
        "d.fieldName": "value"
      },
      "evaluationConditions": {
        "d.fieldName": {
          "$in": ["value1", "value2"]
        }
      },
      "attributes": {
        "cnt": {
          "$sum": 1
        }
      }
    }
  ],
  "metadata": {
    "totalCounters": 3333,
    "errors": 0,
    "warnings": 0,
    "generatedAt": "2025-01-27T..."
  }
}
```

## Особенности

1. **Типизация значений**: MessageTypeID всегда числа, операции сравнения автоматически определяют тип
2. **Обработка ошибок**: Неизвестные операторы логируются в комментарии
3. **Валидация**: Проверка корректности MongoDB операторов
4. **Экранирование**: Поддержка экранированных кавычек в CSV
5. **Пустые значения**: Символ `∅` преобразуется в `null`

## Примеры преобразований

### Простые условия
```
(MessageTypeID = 61; 50) → {"d.MessageTypeID": {"$in": [61, 50]}}
(msgMode = CI) → {"d.msgMode": "CI"}
(rules =*= atm88.1) → {"d.rules": {"$regex": "atm88.1", "$options": "i"}}
```

### Сложные условия
```
(PAN ≠ [p_basicFieldKey]) → {"$expr": {"$ne": ["$d.PAN", "$d.p_basicFieldKey"]}}
(Timestamp > ([d_dt] + 720d)) → {"$expr": {"$gt": ["$d.Timestamp", {"$dateAdd": {"startDate": "$d.d_dt", "unit": "day", "amount": 720}}]}}
```

### Атрибуты
```
frequency → {"cnt": {"$sum": 1}}
total amount → {"sum": {"$sum": "$d.Amount"}}
distinct values number → {"dst": {"$addToSet": "$d.PAN"}}
```
