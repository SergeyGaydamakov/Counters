# FactGenerator - API конструктора

## Обзор

Конструктор `FactGenerator` теперь работает аналогично `FactMapper` и поддерживает гибкую инициализацию конфигурации полей.

## Сигнатура конструктора

```javascript
constructor(fieldConfigPathOrArray = null, fromDate, toDate, targetSize)
```

### Параметры

- `fieldConfigPathOrArray` - конфигурация полей (см. варианты ниже)
- `fromDate` - начальная дата для генерации фактов (по умолчанию: год назад)
- `toDate` - конечная дата для генерации фактов (по умолчанию: сейчас)
- `targetSize` - целевой размер JSON в байтах (опционально)

## Варианты инициализации

### 1. Файл по умолчанию (null)

```javascript
const generator = new FactGenerator();
// Использует fieldConfig.json из корневой директории
```

### 2. Имя файла конфигурации

```javascript
const generator = new FactGenerator('myConfig.json');
// Загружает конфигурацию из указанного файла
```

### 3. Структура конфигурации (массив)

```javascript
const config = [
    {
        "src": "f1",
        "dst": "f1",
        "types": ["user_action", "system_event"]
    },
    {
        "src": "f2",
        "dst": "f2", 
        "types": ["user_action", "payment"]
    }
];

const generator = new FactGenerator(config);
// Использует переданную структуру конфигурации
```

## Структура конфигурации полей

Каждый элемент конфигурации должен содержать:

```javascript
{
    "src": "f1",           // Исходное имя поля (обязательно, string)
    "dst": "f1",           // Целевое имя поля (обязательно, string)  
    "types": ["type1"]     // Массив типов фактов (обязательно, Array<string>)
}
```

## Валидация

Конструктор автоматически валидирует конфигурацию:

- ✅ Проверяет, что конфигурация является массивом
- ✅ Проверяет, что каждый элемент содержит обязательные поля
- ✅ Проверяет типы данных полей
- ✅ Проверяет, что массив `types` не пустой

## Обработка ошибок

### Файл не найден
```javascript
try {
    const generator = new FactGenerator('nonexistent.json');
} catch (error) {
    console.log(error.message); // "Ошибка загрузки конфигурации: Файл конфигурации не найден: nonexistent.json"
}
```

### Неверная структура конфигурации
```javascript
const invalidConfig = [
    {
        "src": "f1",
        "types": "not_an_array" // Ошибка!
    }
];

try {
    const generator = new FactGenerator(invalidConfig);
} catch (error) {
    console.log(error.message); // "Поле конфигурации 0 должно содержать непустой массив 'types'"
}
```

## Примеры использования

### Базовое использование
```javascript
const FactGenerator = require('./src/generators/factGenerator');

// Использование файла по умолчанию
const generator1 = new FactGenerator();

// Использование кастомной конфигурации
const config = [/* ... */];
const generator2 = new FactGenerator(config);

// Генерация фактов
const fact = generator2.generateFact('user_action');
```

### С дополнительными параметрами
```javascript
const fromDate = new Date('2024-01-01');
const toDate = new Date('2024-12-31');
const targetSize = 1024; // 1KB

const generator = new FactGenerator(config, fromDate, toDate, targetSize);
```

## Совместимость с FactMapper

API конструктора теперь полностью совместим с `FactMapper`:

```javascript
// Оба класса используют одинаковый паттерн
const factMapper = new FactMapper(configPathOrArray);
const factGenerator = new FactGenerator(configPathOrArray);
```
