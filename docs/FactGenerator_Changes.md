# Изменения в FactGenerator

## Обзор изменений

Класс `FactGenerator` был полностью переработан для работы с новой структурой конфигурации полей.

## Основные изменения

### 1. Новый конструктор

**Было:**
```javascript
constructor(_fieldCount = 23, _typeCount = 5, _fieldsPerType = 10, _typeFieldsConfig = null, fromDate, toDate, targetSize)
```

**Стало:**
```javascript
constructor(fieldConfig = null, fromDate, toDate, targetSize)
```

### 2. Поддержка конфигурации полей

Теперь конструктор принимает:
- `null` - использует файл `fieldConfig.json` по умолчанию
- `string` - имя файла конфигурации
- `Array` - структуру конфигурации полей

### 3. Тип факта как строка

**Было:** `t: number` (1, 2, 3, ...)
**Стало:** `t: string` ("type1", "type2", "user_action", ...)

### 4. Генерация полей на основе конфигурации

Поля для факта теперь генерируются на основе поля `src` из конфигурации, а не случайным образом.

## Структура конфигурации полей

```json
[
  {
    "src": "f1",
    "dst": "f1", 
    "types": ["type1", "type2", "type3"]
  },
  {
    "src": "f2",
    "dst": "f2",
    "types": ["type1", "type4"]
  }
]
```

- `src` - исходное имя поля (используется в факте)
- `dst` - целевое имя поля (для совместимости)
- `types` - массив типов фактов, для которых это поле используется

## Примеры использования

### 1. Использование файла по умолчанию
```javascript
const generator = new FactGenerator();
const fact = generator.generateFact('type1');
```

### 2. Использование кастомной конфигурации
```javascript
const config = [
  {
    "src": "f1",
    "dst": "f1",
    "types": ["user_action", "system_event"]
  }
];
const generator = new FactGenerator(config);
const fact = generator.generateFact('user_action');
```

### 3. Использование файла конфигурации
```javascript
const generator = new FactGenerator('myConfig.json');
const fact = generator.generateFact('type1');
```

## Новые свойства класса

- `availableFields` - массив доступных полей из конфигурации
- `availableTypes` - массив доступных типов фактов
- `typeFieldsMap` - карта полей для каждого типа

## Обратная совместимость

Старый API больше не поддерживается. Необходимо обновить код, использующий `FactGenerator`.
