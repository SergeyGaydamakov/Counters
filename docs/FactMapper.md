# FactMapper - Модуль преобразования фактов

## Описание

`FactMapper` - это модуль для преобразования входных фактов во внутреннюю сохраняемую структуру на основе конфигурации маппинга. Модуль использует JSON-файл с правилами маппинга для определения того, как поля входного факта должны быть преобразованы в поля внутренней структуры.

## Структура конфигурации

Конфигурация маппинга хранится в файле `factConfigs.json` и имеет следующую структуру:

```json
[
  {
    "src": "source attribute name",
    "dst": "destination field name", 
    "types": ["fact type name 1", "fact type name 2"]
  }
]
```

### Поля конфигурации

- `src` (string) - Имя исходного атрибута во входном факте
- `dst` (string) - Имя целевого поля во внутренней структуре
- `types` (Array<string>) - Массив типов фактов, для которых применяется данное правило маппинга

## Использование

### Создание экземпляра

```javascript
const FactMapper = require('./generators/factMapper');

// Создание с конфигурацией по умолчанию (factConfigs.json в корне проекта)
const mapper = new FactMapper();

// Создание с указанием пути к конфигурации
const mapper = new FactMapper('/path/to/custom/config.json');

// Создание с массивом конфигурации (удобно для тестирования)
const customConfig = [
    {
        src: 'field1',
        dst: 'mapped_field1',
        types: ['type1', 'type2']
    }
];
const mapper = new FactMapper(customConfig);
```

### Преобразование одного факта

```javascript
const inputFact = {
    f1: 'test_value',
    f2: 'another_value',
    otherField: 'ignored'
};

// Маппинг с сохранением неотображенных полей (по умолчанию)
const mappedFact = mapper.mapFact(inputFact, 'type1');

// Маппинг с удалением неотображенных полей
const mappedFactStrict = mapper.mapFact(inputFact, 'type1', false);
```

### Преобразование массива фактов

```javascript
const inputFacts = [
    { f1: 'value1' },
    { f1: 'value2' },
    { f1: 'value3' }
];

const mappedFacts = mapper.mapFacts(inputFacts, 'type1');
```

## API

### Методы

#### `mapFact(inputFact, factType, keepUnmappedFields = true)`
Преобразует входной факт во внутреннюю структуру.

**Параметры:**
- `inputFact` (Object) - Входной факт для преобразования
- `factType` (string) - Тип факта для определения применимых правил
- `keepUnmappedFields` (boolean) - Если true, поля, не найденные в правилах маппинга, сохраняются в результате. Если false, такие поля удаляются из результата. По умолчанию true.

**Возвращает:** Object - Преобразованный факт

**Исключения:**
- `Error` - Если входной факт невалиден или тип факта не поддерживается

#### `mapFacts(inputFacts, factType)`
Преобразует массив входных фактов во внутреннюю структуру.

**Параметры:**
- `inputFacts` (Array<Object>) - Массив входных фактов
- `factType` (string) - Тип фактов для определения применимых правил

**Возвращает:** Array<Object> - Массив преобразованных фактов

#### `getMappingRulesForType(factType)`
Получает правила маппинга для конкретного типа факта.

**Параметры:**
- `factType` (string) - Тип факта

**Возвращает:** Array<Object> - Массив правил маппинга

## Логирование

Модуль использует систему логирования проекта (`Logger`) для вывода информационных сообщений, предупреждений и ошибок.

## Обработка ошибок

- Валидация входных параметров
- Проверка существования файла конфигурации
- Валидация структуры конфигурации
- Обработка ошибок при маппинге отдельных фактов в массиве

## Примеры

См. файлы:
- `src/examples/factMapperExample.js` - Примеры использования
- `src/tests/factMapperTest.js` - Тесты модуля

## Требования

- Node.js
- Модуль `Logger` из `../utils/logger`
- Файл конфигурации `factConfigs.json`
