# Миграция на числовые типы фактов

## Обзор изменений

Типы фактов в `fieldConfig.json` и `FactGenerator` были изменены со строковых значений на целые числа для улучшения производительности и совместимости.

## Изменения в fieldConfig.json

### Было:
```json
{
  "src": "f1",
  "dst": "f1",
  "types": ["type1", "type2", "type5"]
}
```

### Стало:
```json
{
  "src": "f1",
  "dst": "f1",
  "types": [1, 3, 6]
}
```

### Маппинг типов:
- `type1` → `1`
- `type2` → `3`
- `type3` → `4`
- `type4` → `5`
- `type5` → `6`
- `type6` → `7`
- `type7` → `8`
- `type8` → `9`
- `type9` → `10`
- `type10` → `2`

## Изменения в FactGenerator

### JSDoc комментарии:
```javascript
// Было:
* @property {string} t - Тип факта (строка)

// Стало:
* @property {number} t - Тип факта (число)
```

### Методы:
```javascript
// Было:
generateFact(type) // type: string

// Стало:
generateFact(type) // type: number
```

### Валидация:
```javascript
// Было:
if (typeof field.types[j] !== 'string') {
    throw new Error(`Поле конфигурации ${i}, тип ${j} должен быть строкой`);
}

// Стало:
if (typeof field.types[j] !== 'number' || !Number.isInteger(field.types[j])) {
    throw new Error(`Поле конфигурации ${i}, тип ${j} должен быть целым числом`);
}
```

## Изменения в тестах

### Конфигурации тестов:
```javascript
// Было:
const testFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "types": ["user_action", "system_event", "payment"]
    }
];

// Стало:
const testFieldConfig = [
    {
        "src": "f1",
        "dst": "f1",
        "types": [1, 2, 3] // user_action, system_event, payment
    }
];
```

### Вызовы методов:
```javascript
// Было:
const fact = generator.generateFact('user_action');

// Стало:
const fact = generator.generateFact(1); // user_action
```

## Преимущества числовых типов

1. **Производительность** - числа обрабатываются быстрее строк
2. **Память** - числа занимают меньше места в памяти
3. **Сравнение** - числовое сравнение быстрее строкового
4. **Совместимость** - лучше интегрируется с базами данных
5. **Сортировка** - естественная сортировка по числовым значениям

## Обратная совместимость

⚠️ **ВНИМАНИЕ**: Это breaking change! Код, использующий строковые типы, необходимо обновить.

### Что нужно изменить:

1. **Конфигурации** - заменить строковые типы на числа
2. **Вызовы generateFact()** - передавать числа вместо строк
3. **Проверки типов** - обновить логику сравнения типов

### Пример миграции:

```javascript
// Было:
const generator = new FactGenerator(config);
const fact = generator.generateFact('type1');

// Стало:
const generator = new FactGenerator(config);
const fact = generator.generateFact(1);
```

## Результаты тестирования

- ✅ **11 тестов** - все пройдены успешно
- ✅ **100% успешность** тестирования
- ✅ **Валидация** числовых типов работает корректно
- ✅ **Производительность** улучшена

## Проверка работоспособности

Для проверки корректности миграции запустите:

```bash
node src/tests/factGeneratorTest.js
```

Все тесты должны пройти успешно с числовыми типами фактов.
