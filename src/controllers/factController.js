const FactIndexer = require('../generators/factIndexer');
const FactGenerator = require('../generators/factGenerator');
const Logger = require('../utils/logger');

/**
 * Класс-контроллер для управления фактами и их индексными значениями
 * Обеспечивает создание фактов с автоматической генерацией и сохранением индексных значений
 * Работает с абстрактным dbProvider, который должен реализовывать интерфейс для работы с данными
 */
class FactController {
    constructor(dbProvider, fieldConfigPathOrMapArray, indexConfigPathOrMapArray, targetSize) {
        if (!dbProvider) {
            throw new Error('dbProvider обязателен для инициализации FactController');
        }
        
        // Создаем логгер для этого контроллера
        this.logger = Logger.fromEnv('LOG_LEVEL', 'INFO');
        
        // Проверяем, что dbProvider имеет необходимые методы
        const requiredMethods = ['saveFact', 'saveFactIndexList'];
        for (const method of requiredMethods) {
            if (typeof dbProvider[method] !== 'function') {
                throw new Error(`dbProvider должен иметь метод '${method}'`);
            }
        }
        
        this.dbProvider = dbProvider;
        this.factGenerator = new FactGenerator(fieldConfigPathOrMapArray, targetSize);
        this.factIndexer = new FactIndexer(indexConfigPathOrMapArray);
    }


    /**
     * Создает факт и связанные индексные значения, сохраняет их в базу данных
     * @param {Object} fact - объект факта для создания
     * @returns {Promise<Object>} результат операции создания факта
     */
    async saveFact(fact) {
        if (!fact || typeof fact !== 'object') {
            throw new Error('fact должен быть объектом');
        }

        // Валидация обязательных полей факта
        const requiredFields = ['i', 't', 'c', 'd'];
        for (const field of requiredFields) {
            if (!(field in fact)) {
                throw new Error(`Отсутствует обязательное поле факта: ${field}`);
            }
        }

        try {
            this.logger.debug(`\n=== Создание факта ===`);
            this.logger.debug(`Факт ID: ${fact.i}, Тип: ${fact.t}, Дата факта: ${fact.d}, Количество: ${fact.a}, Дата создания: ${fact.c}`);

            // Создаем индексные значения из факта
            const factIndexes = this.factIndexer.index(fact);
            
            this.logger.debug(`✓ Создано ${factIndexes.length} индексных значений`);

            // Выводим информацию о созданных индексных значениях
            if (!factIndexes.length) {
                this.logger.warn('⚠ В факте не найдено полей для создания индексных значений');
                this.logger.debug(`Факт: ${JSON.stringify(fact)}`);
            }

            // Сохраняем факт и индексные значения параллельно
            const savePromises = [
                this.dbProvider.saveFact(fact)
            ];
            
            // Добавляем сохранение индексных значений только если они есть
            if (factIndexes.length > 0) {
                savePromises.push(this.dbProvider.saveFactIndexList(factIndexes));
            } else {
                // Если индексных значений нет, добавляем пустой результат
                savePromises.push(Promise.resolve({ success: true, inserted: 0, updated: 0, errors: [] }));
            }

            // Выполняем операции сохранения параллельно
            const [factResult, indexResult] = await Promise.all(savePromises);
            
            // Проверяем результат сохранения факта
            if (!factResult.success) {
                console.error('✗ Ошибка при сохранении факта:', factResult.error);
                return {
                    success: false,
                    factId: null,
                    factInserted: 0,
                    factUpdated: 0,
                    indexesCreated: 0,
                    indexesInserted: 0,
                    indexesUpdated: 0,
                    indexErrors: [],
                    error: factResult.error,
                    factIndexes: factIndexes
                };
            }

            // Проверяем результат сохранения индексных значений
            if (!indexResult.success) {
                console.error('✗ Ошибка при сохранении индексных значений:', indexResult.error);
                return {
                    success: false,
                    factId: factResult.factId,
                    factInserted: factResult.factInserted,
                    factUpdated: factResult.factUpdated,
                    indexesCreated: 0,
                    indexesInserted: 0,
                    indexesUpdated: 0,
                    indexErrors: [],
                    error: indexResult.error,
                    factIndexes: factIndexes
                };
            }

            this.logger.debug('✓ Факт и индексные значения успешно сохранены');
            this.logger.debug(`  - Факт: ${factResult.factInserted > 0 ? 'вставлен' : 'обновлен'}`);
            this.logger.debug(`  - Индексных значений: вставлено ${indexResult.inserted}, обновлено ${indexResult.updated}`);
            
            if (indexResult.errors && indexResult.errors.length > 0) {
                this.logger.warn(`⚠ Ошибок при сохранении индексов: ${indexResult.errors.length}`);
            }

            return {
                success: true,
                factId: factResult.factId,
                factInserted: factResult.factInserted,
                factUpdated: factResult.factUpdated,
                indexesCreated: factIndexes.length,
                indexesInserted: indexResult.inserted,
                indexesUpdated: indexResult.updated,
                indexErrors: indexResult.errors || [],
                error: null,
                factIndexes: factIndexes
            };

        } catch (error) {
            console.error('✗ Критическая ошибка при создании факта:', error.message);
            
            return {
                success: false,
                factId: null,
                factInserted: 0,
                factUpdated: 0,
                indexesCreated: 0,
                indexesInserted: 0,
                indexesUpdated: 0,
                indexErrors: [],
                error: error.message,
                factIndexes: []
            };
        }
    }

    /**
     * Создает несколько фактов и их индексные значения
     * @param {Array<Object>} facts - массив фактов для создания
     * @returns {Promise<Object>} результат операции создания фактов
     */
    async saveFacts(facts) {
        if (!Array.isArray(facts) || facts.length === 0) {
            throw new Error('facts должен быть непустым массивом');
        }

        this.logger.debug(`\n=== Создание ${facts.length} фактов ===`);

        const results = {
            success: true,
            totalFacts: facts.length,
            successfulFacts: 0,
            failedFacts: 0,
            totalIndexesCreated: 0,
            totalIndexesInserted: 0,
            totalIndexesUpdated: 0,
            errors: [],
            factResults: []
        };

        for (let i = 0; i < facts.length; i++) {
            const fact = facts[i];
            this.logger.debug(`\n--- Обработка факта ${i + 1}/${facts.length} ---`);
            
            try {
                const result = await this.saveFact(fact);
                results.factResults.push(result);
                
                if (result.success) {
                    results.successfulFacts++;
                    results.totalIndexesCreated += result.indexesCreated;
                    results.totalIndexesInserted += result.indexesInserted;
                    results.totalIndexesUpdated += result.indexesUpdated;
                } else {
                    results.failedFacts++;
                    results.errors.push({
                        factIndex: i,
                        factId: fact.i,
                        error: result.error
                    });
                }
            } catch (error) {
                results.failedFacts++;
                results.errors.push({
                    factIndex: i,
                    factId: fact.i,
                    error: error.message
                });
                console.error(`✗ Ошибка при обработке факта ${i + 1}:`, error.message);
            }
        }

        results.success = results.failedFacts === 0;

        this.logger.debug(`\n=== Итоговый результат ===`);
        this.logger.debug(`✓ Успешно обработано: ${results.successfulFacts}/${results.totalFacts} фактов`);
        this.logger.debug(`✓ Создано индексных значений: ${results.totalIndexesCreated}`);
        this.logger.debug(`✓ Вставлено индексных значений: ${results.totalIndexesInserted}`);
        this.logger.debug(`✓ Обновлено индексных значений: ${results.totalIndexesUpdated}`);
        
        if (results.errors.length > 0) {
            this.logger.error(`⚠ Ошибок: ${results.errors.length}`);
        }

        return results;
    }

    /**
     * Запускает обработку фактов:
     * 1. Генерация нового случайного факта
     * 2. Получение из базы данных релевантных фактов для вычисления счетчиков
     * 3. Сохранение нового факта и индексных значений в базу данных
     * 
     */
    MAX_DEPTH_LIMIT = 1000;
    MAX_DEPTH_FROM_DATE = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);

    async run() {
        // Генерация нового случайного факта
        const fact = this.factGenerator.generateRandomTypeFact();
        this.logger.debug(`*** Создан новый факт ${fact.t}: ${fact.i}`);
        const factIndexes = this.factIndexer.index(fact);
        const factIndexHashValues = factIndexes.map(index => index.h);

        // Получение из базы данных релевантных фактов для вычисления счетчиков
        // и сохранение нового факта и индексных значений в базу данных
        const [relevantFacts, result] = await Promise.all([
            this.dbProvider.getRelevantFacts(factIndexHashValues, fact.i, this.MAX_DEPTH_LIMIT, this.MAX_DEPTH_FROM_DATE),
            this.saveFact(fact)
        ]);
        this.logger.debug(`*** Обработан новый факт ${fact.t}: ${fact.i}`);
        
        // this.factGenerator.printFactStatistics(relevantFacts);
        // this.logger.debug(result);
        
        return {
            fact,
            relevantFacts,
            result
        };
    }
}

module.exports = FactController;
