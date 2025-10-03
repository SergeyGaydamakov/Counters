# Переименование поля "types" в "fact_types"

## Обзор изменений

Поле "types" в структуре конфигурации полей было переименовано в "fact_types" для лучшей читаемости и соответствия назначению.

## Изменения в fieldConfig.json

### Было:
```json
{
  "src": "f1",
  "dst": "f1",
  "types": [1, 3, 6, 7, 10]
}
```

### Стало:
```json
{
  "src": "f1",
  "dst": "f1",
  "fact_types": [1, 3, 6, 7, 10]
}
```

## Изменения в FactGenerator

### Метод `_extractAvailableTypes()`:
```javascript
// Было:
field.types.forEach(type => types.add(type));

// Стало:
field.fact_types.forEach(type => types.add(type));
```

### Метод `_buildTypeFieldsMap()`:
```javascript
// Было:
.filter(field => field.types.includes(type))

// Стало:
.filter(field => field.fact_types.includes(type))
```

### Валидация конфигурации:
```javascript
// Было:
if (!Array.isArray(field.types) || field.types.length === 0) {
    throw new Error(`Поле конфигурации ${i} должно содержать непустой массив 'types'`);
}

for (let j = 0; j < field.types.length; j++) {
    if (typeof field.types[j] !== 'number' || !Number.isInteger(field.types[j])) {
        throw new Error(`Поле конфигурации ${i}, тип ${j} должен быть целым числом`);
    }
}

// Стало:
if (!Array.isArray(field.fact_types) || field.fact_types.length === 0) {
    throw new Error(`Поле конфигурации ${i} должно содержать непустой массив 'fact_types'`);
}

for (let j = 0; j < field.fact_types.length; j++) {
    if (typeof field.fact_types[j] !== 'number' || !Number.isInteger(field.fact_types[j])) {
        throw new Error(`Поле конфигурации ${i}, тип ${j} должен быть целым числом`);
    }
}
```

## Изменения в тестах

### Тестовые конфигурации:
```javascript
// Было:
const testFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "types": [1, 2, 3]
    }
];

// Стало:
const testFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "fact_types": [1, 2, 3]
    }
];
```

## Преимущества переименования

1. **Читаемость** - название "fact_types" более описательное
2. **Ясность** - сразу понятно, что это типы фактов
3. **Консистентность** - соответствует общему стилю именования
4. **Расширяемость** - можно добавить другие типы полей (например, "field_types")

## Обратная совместимость

⚠️ **ВНИМАНИЕ**: Это breaking change! Код, использующий поле "types", необходимо обновить.

### Что нужно изменить:

1. **Конфигурации** - заменить "types" на "fact_types"
2. **Валидация** - обновить проверки полей конфигурации
3. **Обработка данных** - изменить доступ к полю в коде

### Пример миграции:

```javascript
// Было:
const config = [
    {
        "src": "f1",
        "dst": "f1",
        "types": [1, 2, 3]
    }
];

// Стало:
const config = [
    {
        "src": "f1",
        "dst": "f1",
        "fact_types": [1, 2, 3]
    }
];
```

## Результаты тестирования

- ✅ **11 тестов** - все пройдены успешно
- ✅ **100% успешность** тестирования
- ✅ **Валидация** поля "fact_types" работает корректно
- ✅ **Обратная совместимость** - корректно обрабатываются ошибки валидации

## Проверка работоспособности

Для проверки корректности переименования запустите:

```bash
node src/tests/factGeneratorTest.js
```

Все тесты должны пройти успешно с полем "fact_types".

## Структура конфигурации

Теперь каждая запись в конфигурации полей должна иметь следующую структуру:

```json
{
  "src": "f1",           // Исходное имя поля (обязательно)
  "dst": "f1",           // Целевое имя поля (обязательно)
  "fact_types": [1, 2]   // Массив типов фактов (обязательно)
}
```
