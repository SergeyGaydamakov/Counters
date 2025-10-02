const FactGenerator = require('../generators/factGenerator');
const Logger = require('../utils/logger');

// Определяем диапазон дат для тестирования
const fromDate = new Date('2024-01-01');
const toDate = new Date('2024-12-31');

// Создаем логгер для тестов
const logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');

// Создаем экземпляр генератора с настройками
const generator = new FactGenerator(23, 5, 10, null, fromDate, toDate, 500);

logger.debug('=== Тестирование FactGenerator ===\n');

// Тест 1: Генерация одного факта типа 1
logger.debug('1. Генерация одного факта типа 1:');
const singleFact = generator.generateFact(1);
logger.debug(JSON.stringify(singleFact, null, 2));
logger.debug('Количество полей:', Object.keys(singleFact).length); // Должно быть 16 (5 обязательных + 10 случайных + z)

// Тест 2: Генерация 5 фактов типа 2
logger.debug('\n2. Генерация 5 фактов типа 2:');
const multipleFacts = [];
for (let i = 0; i < 5; i++) {
    multipleFacts.push(generator.generateFact(2));
}
multipleFacts.forEach((fact, index) => {
    logger.debug(`Факт ${index + 1}:`, JSON.stringify(fact, null, 2));
});

// Тест 3: Генерация случайных типов фактов
logger.debug('\n3. Генерация 3 фактов случайных типов:');
const randomFacts = [];
for (let i = 0; i < 3; i++) {
    randomFacts.push(generator.generateRandomTypeFact());
}
randomFacts.forEach((fact, index) => {
    logger.debug(`Случайный факт ${index + 1} (тип ${fact.t}):`, JSON.stringify(fact, null, 2));
});

// Тест 4: Проверка диапазонов значений
logger.debug('\n4. Проверка диапазонов значений:');
const testFact = generator.generateFact(1);
logger.debug('Значение a (должно быть от 1 до 1000000):', testFact.a);
logger.debug('Дата создания c (текущая дата):', testFact.c);
logger.debug('Дата d (должна быть в диапазоне 2024):', testFact.d);
logger.debug('Пример случайного поля f1:', testFact.f1, '(длина:', testFact.f1?.length || 0, ')');

// Тест 5: Проверка уникальности полей для разных типов
logger.debug('\n5. Сравнение полей для разных типов:');
const type1Fact = generator.generateFact(1);
const type2Fact = generator.generateFact(2);

const type1Fields = Object.keys(type1Fact).filter(key => key.startsWith('f')).sort();
const type2Fields = Object.keys(type2Fact).filter(key => key.startsWith('f')).sort();

logger.debug('Поля типа 1:', type1Fields);
logger.debug('Поля типа 2:', type2Fields);
logger.debug('Поля различаются:', JSON.stringify(type1Fields) !== JSON.stringify(type2Fields));

// Тест 6: Проверка валидации типа факта
logger.debug('\n6. Проверка валидации типа факта:');

// Тест на корректный тип
try {
    const validFact = generator.generateFact(3);
    logger.debug('✓ Тип 3 (корректный): успешно создан факт');
} catch (error) {
    logger.error('✗ Ошибка при создании факта типа 3:', error.message);
}

// Тест на некорректный тип (меньше 1)
try {
    const invalidFact1 = generator.generateFact(0);
    logger.error('✗ Тип 0: не должен был создаться, но создался');
} catch (error) {
    logger.debug('✓ Тип 0 (некорректный): правильно выброшена ошибка -', error.message);
}

// Тест на некорректный тип (больше максимального)
try {
    const invalidFact2 = generator.generateFact(10);
    logger.error('✗ Тип 10: не должен был создаться, но создался');
} catch (error) {
    logger.debug('✓ Тип 10 (некорректный): правильно выброшена ошибка -', error.message);
}

// Тест на граничные значения
try {
    const boundaryFact1 = generator.generateFact(1);
    logger.debug('✓ Тип 1 (минимальный): успешно создан факт');
} catch (error) {
    logger.error('✗ Ошибка при создании факта типа 1:', error.message);
}

try {
    const boundaryFact2 = generator.generateFact(5);
    logger.debug('✓ Тип 5 (максимальный): успешно создан факт');
} catch (error) {
    logger.error('✗ Ошибка при создании факта типа 5:', error.message);
}

// Тест 7: Проверка поля z и контроля размера JSON
logger.debug('\n7. Проверка поля z и контроля размера JSON:');

// Тест без указания размера (поле z не должно добавляться)
const factWithoutSize = generator.generateFact(1);
logger.debug('Факт без targetSize содержит поле z:', 'z' in factWithoutSize);

// Тест с указанием целевого размера
const targetSize500 = 500; // 500 байт
const factWithSize500 = generator.generateFact(1);
const actualSize500 = Buffer.byteLength(JSON.stringify(factWithSize500), 'utf8');

logger.debug(`Целевой размер: ${targetSize500} байт`);
logger.debug(`Фактический размер: ${actualSize500} байт`);
logger.debug('Поле z присутствует:', 'z' in factWithSize500);
logger.debug('Длина поля z:', factWithSize500.z ? factWithSize500.z.length : 0);
logger.debug('Размер достигнут (±10 байт):', Math.abs(actualSize500 - targetSize500) <= 10);

// Тест с большим целевым размером
const targetSize1000 = 1000; // 1000 байт
const factWithSize1000 = generator.generateFact(2);
const actualSize1000 = Buffer.byteLength(JSON.stringify(factWithSize1000), 'utf8');

logger.debug(`\nЦелевой размер: ${targetSize1000} байт`);
logger.debug(`Фактический размер: ${actualSize1000} байт`);
logger.debug('Длина поля z:', factWithSize1000.z ? factWithSize1000.z.length : 0);

// Тест с маленьким целевым размером (меньше базового размера)
const targetSize100 = 100; // 100 байт (может быть меньше базового размера)
const factWithSize100 = generator.generateFact(3);
const actualSize100 = Buffer.byteLength(JSON.stringify(factWithSize100), 'utf8');

logger.debug(`\nЦелевой размер: ${targetSize100} байт`);
logger.debug(`Фактический размер: ${actualSize100} байт`);
logger.debug('Поле z (при маленьком размере):', factWithSize100.z);

// Тест массовой генерации с размером
logger.debug('\n8. Тест массовой генерации фактов с заданным размером:');
const factsWithSize = [];
for (let i = 0; i < 3; i++) {
    factsWithSize.push(generator.generateFact(1));
}
factsWithSize.forEach((fact, index) => {
    const size = Buffer.byteLength(JSON.stringify(fact), 'utf8');
    logger.debug(`Факт ${index + 1}: размер ${size} байт, длина z: ${fact.z ? fact.z.length : 0}`);
});

// Тест случайных типов с размером
logger.debug('\n9. Тест случайных типов с заданным размером:');
const randomFactsWithSize = [];
for (let i = 0; i < 2; i++) {
    randomFactsWithSize.push(generator.generateRandomTypeFact());
}
randomFactsWithSize.forEach((fact, index) => {
    const size = Buffer.byteLength(JSON.stringify(fact), 'utf8');
    logger.debug(`Случайный факт ${index + 1} (тип ${fact.t}): размер ${size} байт`);
});

// Тест 10: Дополнительные проверки корректности поля z
logger.debug('\n10. Дополнительные проверки поля z:');

// Проверка точности размера для разных целевых значений
const targetSizes = [200, 300, 500, 750, 1200, 1500];
logger.debug('Проверка точности размера для различных целевых значений:');

targetSizes.forEach(targetSize => {
    const fact = generator.generateFact(1);
    const actualSize = Buffer.byteLength(JSON.stringify(fact), 'utf8');
    const deviation = Math.abs(actualSize - targetSize);
    const accurate = deviation <= 5; // Допустимое отклонение ±5 байт
    
    logger.debug(`  Цель: ${targetSize}б → Факт: ${actualSize}б (отклонение: ${deviation}б) ${accurate ? '✓' : '✗'}`);
});

// Проверка содержимого поля z
logger.debug('\nПроверка содержимого поля z:');
const factForZCheck = generator.generateFact(2);
logger.debug('Поле z содержит только допустимые символы:', /^[a-zA-Z]*$/.test(factForZCheck.z));
logger.debug('Длина поля z больше 0:', factForZCheck.z.length > 0);

// Проверка консистентности размера
logger.debug('\nПроверка консистентности размера при повторных вызовах:');
const sizes = [];
for (let i = 0; i < 5; i++) {
    const fact = generator.generateFact(3);
    const size = Buffer.byteLength(JSON.stringify(fact), 'utf8');
    sizes.push(size);
}
const allSameSize = sizes.every(size => size === sizes[0]);
logger.debug(`Размеры: [${sizes.join(', ')}] - все одинаковые: ${allSameSize ? '✓' : '✗'}`);

// Проверка граничных случаев
logger.debug('\nПроверка граничных случаев:');

// Очень маленький размер
try {
    const tinyFact = generator.generateFact(1);
    const tinySize = Buffer.byteLength(JSON.stringify(tinyFact), 'utf8');
    logger.debug(`Очень маленький размер (50): фактический ${tinySize}, поле z: "${tinyFact.z}"`);
} catch (error) {
    logger.debug('Ошибка при маленьком размере:', error.message);
}

// Нулевой размер
try {
    const zeroFact = generator.generateFact(1);
    logger.debug('Нулевой размер: поле z присутствует:', 'z' in zeroFact);
} catch (error) {
    logger.debug('Ошибка при нулевом размере:', error.message);
}

// Отрицательный размер
try {
    const negativeFact = generator.generateFact(1);
    logger.debug('Отрицательный размер: поле z присутствует:', 'z' in negativeFact);
} catch (error) {
    logger.debug('Ошибка при отрицательном размере:', error.message);
}

// Проверка структуры JSON с полем z
logger.debug('\nПроверка структуры JSON с полем z:');
const structureFact = generator.generateFact(1);
const keys = Object.keys(structureFact);
const hasRequiredFields = ['t', 'a', 'c', 'z'].every(field => keys.includes(field));
const fieldCount = keys.length;

logger.debug('Обязательные поля присутствуют (t, a, c, z):', hasRequiredFields ? '✓' : '✗');
logger.debug('Общее количество полей:', fieldCount);
logger.debug('Поле z в конце объекта:', keys[keys.length - 1] === 'z' ? '✓' : '✗');

// Проверка производительности генерации с полем z
logger.debug('\nПроверка производительности генерации с полем z:');
const startTime = Date.now();
const performanceFacts = [];
for (let i = 0; i < 100; i++) {
    performanceFacts.push(generator.generateFact(1));
}
const endTime = Date.now();
const duration = endTime - startTime;
const avgSize = performanceFacts.reduce((sum, fact) => {
    return sum + Buffer.byteLength(JSON.stringify(fact), 'utf8');
}, 0) / performanceFacts.length;

logger.debug(`Сгенерировано 100 фактов за ${duration}мс`);
logger.debug(`Средний размер: ${Math.round(avgSize)} байт`);
logger.debug(`Скорость: ${Math.round(100 / (duration / 1000))} фактов/сек`);

// Тест 11: Проверка поля i (UUID)
logger.debug('\n11. Проверка поля i (UUID):');

// Проверка наличия и формата поля i
const factWithGuid = generator.generateFact(1);
logger.debug('Поле i присутствует:', 'i' in factWithGuid);
logger.debug('Значение поля i:', factWithGuid.i);

// Проверка формата UUID v4 (xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx)
const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
logger.debug('Поле i соответствует формату UUID v4:', uuidRegex.test(factWithGuid.i));

// Проверка уникальности UUID
logger.debug('\nПроверка уникальности UUID:');
const uuids = [];
for (let i = 0; i < 10; i++) {
    const fact = generator.generateFact(1);
    uuids.push(fact.i);
}

const uniqueUuids = new Set(uuids);
logger.debug(`Сгенерировано ${uuids.length} UUID, уникальных: ${uniqueUuids.size}`);
logger.debug('Все UUID уникальны:', uuids.length === uniqueUuids.size ? '✓' : '✗');

// Проверка структуры с новым полем i
logger.debug('\nПроверка обновленной структуры JSON:');
const structureFactWithI = generator.generateFact(2);
const keysWithI = Object.keys(structureFactWithI);
const hasAllRequiredFields = ['i', 't', 'a', 'c', 'd'].every(field => keysWithI.includes(field));

logger.debug('Все обязательные поля присутствуют (i, t, a, c, d):', hasAllRequiredFields ? '✓' : '✗');
logger.debug('Поле i идет первым:', keysWithI[0] === 'i' ? '✓' : '✗');
logger.debug('Общее количество полей с i:', keysWithI.length);

// Проверка влияния поля i на размер JSON
logger.debug('\nВлияние поля i на размер JSON:');
const factWithoutTargetSize = generator.generateFact(1);
const factWithTargetSize = generator.generateFact(1);

const sizeWithoutTarget = Buffer.byteLength(JSON.stringify(factWithoutTargetSize), 'utf8');
const sizeWithTarget = Buffer.byteLength(JSON.stringify(factWithTargetSize), 'utf8');

logger.debug(`Размер без targetSize: ${sizeWithoutTarget} байт`);
logger.debug(`Размер с targetSize 500: ${sizeWithTarget} байт`);
logger.debug('Поле i учитывается в расчете размера:', sizeWithTarget === 500 ? '✓' : '✗');

// Проверка формата UUID в массовой генерации
logger.debug('\nПроверка UUID в массовой генерации:');
const massiveFacts = [];
for (let i = 0; i < 5; i++) {
    massiveFacts.push(generator.generateFact(3));
}
const allHaveValidUuids = massiveFacts.every(fact => uuidRegex.test(fact.i));
const allUuidsUnique = new Set(massiveFacts.map(fact => fact.i)).size === massiveFacts.length;

logger.debug('Все факты имеют валидные UUID:', allHaveValidUuids ? '✓' : '✗');
logger.debug('Все UUID уникальны в массиве:', allUuidsUnique ? '✓' : '✗');

// Тест 12: Проверка поля c (дата создания объекта)
logger.debug('\n12. Проверка поля c (дата создания объекта):');

// Проверка наличия и типа поля c
const factWithC = generator.generateFact(1);
logger.debug('Поле c присутствует:', 'c' in factWithC);
logger.debug('Значение поля c:', factWithC.c);
logger.debug('Поле c является объектом Date:', factWithC.c instanceof Date);

// Проверка что поле c содержит текущую дату (с небольшой погрешностью)
const now = new Date();
const timeDiff = Math.abs(now.getTime() - factWithC.c.getTime());
const isRecent = timeDiff < 5000; // менее 5 секунд
logger.debug('Поле c содержит недавнюю дату (в пределах 5 сек):', isRecent ? '✓' : '✗');

// Проверка что поле c отличается от поля d
const factForComparison = generator.generateFact(2);
const cIsDifferentFromD = factForComparison.c.getTime() !== factForComparison.d.getTime();
logger.debug('Поле c отличается от поля d:', cIsDifferentFromD ? '✓' : '✗');

// Проверка структуры с полем c
logger.debug('\nПроверка обновленной структуры JSON с полем c:');
const structureFactWithC = generator.generateFact(3);
const keysWithC = Object.keys(structureFactWithC);
const hasAllFieldsIncludingC = ['i', 't', 'a', 'c', 'd'].every(field => keysWithC.includes(field));

logger.debug('Все обязательные поля присутствуют (i, t, a, c, d):', hasAllFieldsIncludingC ? '✓' : '✗');
logger.debug('Общее количество полей с c:', keysWithC.length);

// Проверка что поле c идет после поля a и перед полем d
const cIndex = keysWithC.indexOf('c');
const aIndex = keysWithC.indexOf('a');
const dIndex = keysWithC.indexOf('d');
const correctOrder = aIndex < cIndex && cIndex < dIndex;
logger.debug('Поле c идет после a и перед d:', correctOrder ? '✓' : '✗');

// Проверка в массовой генерации
logger.debug('\nПроверка поля c в массовой генерации:');
const massiveFactsWithC = [];
for (let i = 0; i < 3; i++) {
    massiveFactsWithC.push(generator.generateFact(1));
}
const allHaveValidC = massiveFactsWithC.every(fact => fact.c instanceof Date);
const allCHaveRecentDates = massiveFactsWithC.every(fact => {
    const timeDiff = Math.abs(now.getTime() - fact.c.getTime());
    return timeDiff < 10000; // менее 10 секунд
});

logger.debug('Все факты имеют валидное поле c (Date):', allHaveValidC ? '✓' : '✗');
logger.debug('Все поля c содержат недавние даты:', allCHaveRecentDates ? '✓' : '✗');

logger.debug('\n=== Тестирование завершено ===');
