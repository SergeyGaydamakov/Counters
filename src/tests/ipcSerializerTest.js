/**
 * Тесты для IPC Serializer - утилиты для сериализации/десериализации IPC сообщений
 */

const ipcSerializer = require('../common/ipcSerializer');
const Logger = require('../common/logger');

class IPCSerializerTest {
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
        
        // Сохраняем исходное состояние MessagePack
        this.originalMessagePackState = ipcSerializer.isMessagePackEnabled();
    }

    /**
     * Запуск всех тестов
     */
    async runAllTests() {
        this.logger.debug('=== Тестирование IPC Serializer ===\n');

        try {
            // 1. Базовые тесты JSON
            await this.testJSONSerialization('1. Тест JSON сериализации/десериализации...');
            await this.testJSONComplexObjects('2. Тест JSON с сложными объектами...');
            await this.testJSONDateObjects('3. Тест JSON с Date объектами...');
            
            // 2. Тесты MessagePack (если доступен)
            if (this.canTestMessagePack()) {
                await this.testMessagePackSerialization('4. Тест MessagePack сериализации/десериализации...');
                await this.testMessagePackComplexObjects('5. Тест MessagePack с сложными объектами...');
                await this.testMessagePackDateObjects('6. Тест MessagePack с Date объектами...');
                await this.testMessagePackLargeData('7. Тест MessagePack с большими данными...');
                await this.testMessagePackNestedDates('8. Тест MessagePack с вложенными Date...');
            } else {
                this.logger.debug('   ⚠ MessagePack недоступен, пропускаем тесты MessagePack');
            }
            
            // 3. Тесты функций send/receive
            await this.testSendReceiveJSON('9. Тест send/receive с JSON...');
            if (this.canTestMessagePack()) {
                await this.testSendReceiveMessagePack('10. Тест send/receive с MessagePack...');
            }
            
            // 4. Тесты обратной совместимости
            await this.testBackwardCompatibility('11. Тест обратной совместимости...');
            
            // 5. Тесты обработки ошибок
            await this.testErrorHandling('12. Тест обработки ошибок...');
            
        } catch (error) {
            this.logger.error('Критическая ошибка:', error.message);
        } finally {
            // Восстанавливаем исходное состояние MessagePack
            ipcSerializer.setMessagePackEnabled(this.originalMessagePackState);
            this.printResults();
        }
    }

    /**
     * Проверяет, можно ли тестировать MessagePack
     */
    canTestMessagePack() {
        try {
            require('msgpackr');
            return true;
        } catch (error) {
            return false;
        }
    }

    /**
     * Тест 1: JSON сериализация/десериализация
     */
    async testJSONSerialization(title) {
        this.logger.debug(title);
        
        try {
            // Отключаем MessagePack для теста JSON
            ipcSerializer.setMessagePackEnabled(false);
            
            const testData = {
                type: 'TEST',
                id: 'test-123',
                data: { value: 42, name: 'test' },
                array: [1, 2, 3]
            };
            
            const serialized = ipcSerializer.serialize(testData);
            if (typeof serialized !== 'string') {
                throw new Error('JSON сериализация должна возвращать строку');
            }
            
            const deserialized = ipcSerializer.deserialize(serialized);
            
            if (JSON.stringify(deserialized) !== JSON.stringify(testData)) {
                throw new Error('Данные не совпадают после сериализации/десериализации');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testJSONSerialization: ${error.message}`);
        }
    }

    /**
     * Тест 2: JSON с сложными объектами
     */
    async testJSONComplexObjects(title) {
        this.logger.debug(title);
        
        try {
            ipcSerializer.setMessagePackEnabled(false);
            
            const testData = {
                nested: {
                    level1: {
                        level2: {
                            value: 'deep'
                        }
                    }
                },
                array: [
                    { id: 1, name: 'first' },
                    { id: 2, name: 'second' }
                ],
                mixed: {
                    string: 'text',
                    number: 123,
                    boolean: true,
                    nullValue: null,
                    undefinedValue: undefined
                }
            };
            
            const serialized = ipcSerializer.serialize(testData);
            const deserialized = ipcSerializer.deserialize(serialized);
            
            if (deserialized.nested.level1.level2.value !== 'deep') {
                throw new Error('Вложенные объекты не сохранились');
            }
            
            if (deserialized.array.length !== 2 || deserialized.array[0].id !== 1) {
                throw new Error('Массивы не сохранились');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testJSONComplexObjects: ${error.message}`);
        }
    }

    /**
     * Тест 3: JSON с Date объектами
     */
    async testJSONDateObjects(title) {
        this.logger.debug(title);
        
        try {
            ipcSerializer.setMessagePackEnabled(false);
            
            const testDate = new Date('2024-01-01T00:00:00.000Z');
            const testData = {
                date: testDate,
                dates: [testDate, new Date('2024-12-31T23:59:59.999Z')]
            };
            
            const serialized = ipcSerializer.serialize(testData);
            const deserialized = ipcSerializer.deserialize(serialized);
            
            // JSON сериализует Date как строку, поэтому проверяем строку
            if (typeof deserialized.date !== 'string') {
                throw new Error('Date должен быть сериализован как строка в JSON');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testJSONDateObjects: ${error.message}`);
        }
    }

    /**
     * Тест 4: MessagePack сериализация/десериализация
     */
    async testMessagePackSerialization(title) {
        this.logger.debug(title);
        
        try {
            if (!this.canTestMessagePack()) {
                this.logger.debug('   ⚠ MessagePack недоступен, пропуск');
                return;
            }
            
            ipcSerializer.setMessagePackEnabled(true);
            
            const testData = {
                type: 'TEST',
                id: 'test-123',
                data: { value: 42, name: 'test' },
                array: [1, 2, 3]
            };
            
            const serialized = ipcSerializer.serialize(testData);
            if (!Buffer.isBuffer(serialized)) {
                throw new Error('MessagePack сериализация должна возвращать Buffer');
            }
            
            const deserialized = ipcSerializer.deserialize(serialized);
            
            if (JSON.stringify(deserialized) !== JSON.stringify(testData)) {
                throw new Error('Данные не совпадают после сериализации/десериализации');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testMessagePackSerialization: ${error.message}`);
        }
    }

    /**
     * Тест 5: MessagePack с сложными объектами
     */
    async testMessagePackComplexObjects(title) {
        this.logger.debug(title);
        
        try {
            if (!this.canTestMessagePack()) {
                this.logger.debug('   ⚠ MessagePack недоступен, пропуск');
                return;
            }
            
            ipcSerializer.setMessagePackEnabled(true);
            
            const testData = {
                nested: {
                    level1: {
                        level2: {
                            value: 'deep',
                            number: 12345
                        }
                    }
                },
                array: [
                    { id: 1, name: 'first', active: true },
                    { id: 2, name: 'second', active: false }
                ],
                mixed: {
                    string: 'text',
                    number: 123,
                    boolean: true,
                    nullValue: null
                }
            };
            
            const serialized = ipcSerializer.serialize(testData);
            const deserialized = ipcSerializer.deserialize(serialized);
            
            if (deserialized.nested.level1.level2.value !== 'deep') {
                throw new Error('Вложенные объекты не сохранились');
            }
            
            if (deserialized.array.length !== 2 || deserialized.array[0].id !== 1) {
                throw new Error('Массивы не сохранились');
            }
            
            if (deserialized.mixed.boolean !== true) {
                throw new Error('Булевы значения не сохранились');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testMessagePackComplexObjects: ${error.message}`);
        }
    }

    /**
     * Тест 6: MessagePack с Date объектами
     */
    async testMessagePackDateObjects(title) {
        this.logger.debug(title);
        
        try {
            if (!this.canTestMessagePack()) {
                this.logger.debug('   ⚠ MessagePack недоступен, пропуск');
                return;
            }
            
            ipcSerializer.setMessagePackEnabled(true);
            
            const testDate = new Date('2024-01-01T00:00:00.000Z');
            const testDate2 = new Date('2024-12-31T23:59:59.999Z');
            const testData = {
                date: testDate,
                dates: [testDate, testDate2],
                nested: {
                    date: testDate
                }
            };
            
            const serialized = ipcSerializer.serialize(testData);
            const deserialized = ipcSerializer.deserialize(serialized);
            
            // MessagePack должен сохранить Date как Date объект
            if (!(deserialized.date instanceof Date)) {
                throw new Error('Date должен остаться Date объектом после MessagePack десериализации');
            }
            
            if (deserialized.date.getTime() !== testDate.getTime()) {
                throw new Error('Date значение не сохранилось');
            }
            
            if (!(deserialized.dates[0] instanceof Date)) {
                throw new Error('Date в массиве должен остаться Date объектом');
            }
            
            if (!(deserialized.nested.date instanceof Date)) {
                throw new Error('Date во вложенном объекте должен остаться Date объектом');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testMessagePackDateObjects: ${error.message}`);
        }
    }

    /**
     * Тест 7: MessagePack с большими данными
     */
    async testMessagePackLargeData(title) {
        this.logger.debug(title);
        
        try {
            if (!this.canTestMessagePack()) {
                this.logger.debug('   ⚠ MessagePack недоступен, пропуск');
                return;
            }
            
            ipcSerializer.setMessagePackEnabled(true);
            
            // Создаем большой массив данных
            const largeArray = Array.from({ length: 1000 }, (_, i) => ({
                id: i,
                name: `item-${i}`,
                value: Math.random() * 1000,
                date: new Date(2024, 0, 1 + i)
            }));
            
            const testData = {
                type: 'LARGE',
                items: largeArray
            };
            
            const serialized = ipcSerializer.serialize(testData);
            const deserialized = ipcSerializer.deserialize(serialized);
            
            if (deserialized.items.length !== 1000) {
                throw new Error('Большой массив не сохранился');
            }
            
            if (!(deserialized.items[0].date instanceof Date)) {
                throw new Error('Date в большом массиве не сохранился');
            }
            
            // Проверяем размер - MessagePack должен быть компактнее JSON
            const jsonSize = JSON.stringify(testData).length;
            const msgpackSize = serialized.length;
            
            this.logger.debug(`   JSON размер: ${jsonSize} байт, MessagePack размер: ${msgpackSize} байт`);
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testMessagePackLargeData: ${error.message}`);
        }
    }

    /**
     * Тест 8: MessagePack с вложенными Date
     */
    async testMessagePackNestedDates(title) {
        this.logger.debug(title);
        
        try {
            if (!this.canTestMessagePack()) {
                this.logger.debug('   ⚠ MessagePack недоступен, пропуск');
                return;
            }
            
            ipcSerializer.setMessagePackEnabled(true);
            
            const testDate = new Date('2024-06-15T12:30:45.123Z');
            const testData = {
                level1: {
                    level2: {
                        level3: {
                            date: testDate,
                            dates: [testDate, new Date('2024-01-01')]
                        }
                    }
                },
                array: [
                    { date: testDate },
                    { nested: { date: testDate } }
                ]
            };
            
            const serialized = ipcSerializer.serialize(testData);
            const deserialized = ipcSerializer.deserialize(serialized);
            
            if (!(deserialized.level1.level2.level3.date instanceof Date)) {
                throw new Error('Date в глубоко вложенном объекте не сохранился');
            }
            
            if (!(deserialized.level1.level2.level3.dates[0] instanceof Date)) {
                throw new Error('Date в массиве внутри вложенного объекта не сохранился');
            }
            
            if (!(deserialized.array[0].date instanceof Date)) {
                throw new Error('Date в массиве объектов не сохранился');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testMessagePackNestedDates: ${error.message}`);
        }
    }

    /**
     * Тест 9: send/receive с JSON
     */
    async testSendReceiveJSON(title) {
        this.logger.debug(title);
        
        try {
            ipcSerializer.setMessagePackEnabled(false);
            
            const testMessage = {
                type: 'TEST',
                id: 'test-send-receive',
                data: { value: 42 }
            };
            
            // Мокируем process объект
            let sentData = null;
            const mockProcess = {
                send: (data) => {
                    sentData = data;
                }
            };
            
            ipcSerializer.send(mockProcess, testMessage);
            
            if (!sentData) {
                throw new Error('send() не отправил данные');
            }
            
            // Проверяем, что данные можно получить через receive
            const received = ipcSerializer.receive(sentData);
            
            if (JSON.stringify(received) !== JSON.stringify(testMessage)) {
                throw new Error('Данные не совпадают после send/receive');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testSendReceiveJSON: ${error.message}`);
        }
    }

    /**
     * Тест 10: send/receive с MessagePack
     */
    async testSendReceiveMessagePack(title) {
        this.logger.debug(title);
        
        try {
            if (!this.canTestMessagePack()) {
                this.logger.debug('   ⚠ MessagePack недоступен, пропуск');
                return;
            }
            
            ipcSerializer.setMessagePackEnabled(true);
            
            const testMessage = {
                type: 'TEST',
                id: 'test-send-receive-msgpack',
                data: { value: 42, date: new Date('2024-01-01') }
            };
            
            // Мокируем process объект
            let sentData = null;
            const mockProcess = {
                send: (data) => {
                    sentData = data;
                }
            };
            
            ipcSerializer.send(mockProcess, testMessage);
            
            if (!sentData) {
                throw new Error('send() не отправил данные');
            }
            
            // Проверяем маркер MessagePack
            if (!sentData.__msgpack || !sentData.__data) {
                throw new Error('MessagePack данные должны иметь маркер __msgpack');
            }
            
            // Проверяем, что данные можно получить через receive
            const received = ipcSerializer.receive(sentData);
            
            if (received.type !== testMessage.type || received.id !== testMessage.id) {
                throw new Error('Данные не совпадают после send/receive');
            }
            
            if (!(received.data.date instanceof Date)) {
                throw new Error('Date объект не сохранился через send/receive');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testSendReceiveMessagePack: ${error.message}`);
        }
    }

    /**
     * Тест 11: Обратная совместимость
     */
    async testBackwardCompatibility(title) {
        this.logger.debug(title);
        
        try {
            // Тест: старый формат (обычный объект без маркеров) должен работать
            const oldFormatMessage = {
                type: 'OLD_FORMAT',
                id: 'test-old',
                data: { value: 123 }
            };
            
            // receive должен обработать обычный объект (старый JSON формат)
            const received = ipcSerializer.receive(oldFormatMessage);
            
            if (received.type !== 'OLD_FORMAT' || received.id !== 'test-old') {
                throw new Error('Обратная совместимость не работает');
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testBackwardCompatibility: ${error.message}`);
        }
    }

    /**
     * Тест 12: Обработка ошибок
     */
    async testErrorHandling(title) {
        this.logger.debug(title);
        
        try {
            // Тест: send с некорректным process
            try {
                ipcSerializer.send(null, { type: 'TEST' });
                throw new Error('Должна была быть ошибка для null process');
            } catch (error) {
                if (!error.message.includes('process.send не доступен')) {
                    throw error;
                }
            }
            
            // Тест: send с process без метода send
            try {
                ipcSerializer.send({}, { type: 'TEST' });
                throw new Error('Должна была быть ошибка для process без send');
            } catch (error) {
                if (!error.message.includes('process.send не доступен')) {
                    throw error;
                }
            }
            
            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
            this.testResults.failed++;
            this.testResults.errors.push(`testErrorHandling: ${error.message}`);
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        const total = this.testResults.passed + this.testResults.failed;
        const successRate = total > 0 ? ((this.testResults.passed / total) * 100).toFixed(2) : 0;
        
        this.logger.info('\n=== Результаты тестирования IPC Serializer ===');
        this.logger.info(`Всего тестов: ${total}`);
        this.logger.info(`Успешно: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);
        
        if (this.testResults.errors.length > 0) {
            this.logger.error('\nОшибки:');
            this.testResults.errors.forEach((error, index) => {
                this.logger.error(`  ${index + 1}. ${error}`);
            });
        }
        
        this.logger.info(`\nПроцент успеха: ${successRate}%`);
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new IPCSerializerTest();
    test.runAllTests().catch(console.error);
}

module.exports = IPCSerializerTest;

