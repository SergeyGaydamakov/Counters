const { MongoProvider, FactService } = require('../index');
const Logger = require('../common/logger');
const config = require('../common/config');

/**
 * Тесты для всех методов FactController
 */
class FactControllerTest {
    // Тестовая конфигурация полей фактов
    _testFieldConfig = [
        {
            "src": "dt",
            "dst": "dt",
            "shortDst": "dt",
            "message_types": [1, 2, 3],
            "generator": {
                "type": "date",
                "min": "2025-01-01",
                "max": "2025-10-01"
            }
        },
        {
            "src": "a",
            "dst": "a",
            "shortDst": "a",
            "message_types": [1, 2, 3],
            "generator": {
                "type": "integer",
                "min": 1,
                "max": 10000000,
                "default_value": 1000,
                "default_random": 0.1
            }
        },
        {
            "src": "f1",
            "dst": "f1",
            "shortDst": "f1",
            "message_types": [1, 2, 3],
            "generator": {
                "type": "string",
                "min": 3,
                "max": 20,
                "default_value": "1234567890",
                "default_random": 0.1
            },
            "key_order": 1
        },
        {
            "src": "f2",
            "dst": "f2",
            "shortDst": "f2",
            "message_types": [1, 3],
            "generator": {
                "type": "string",
                "min": 3,
                "max": 20,
                "default_value": "1234567890",
                "default_random": 0.1
            }
        },
        {
            "src": "f3",
            "dst": "f3",
            "shortDst": "f3",
            "message_types": [2, 3],
            "generator": {
                "type": "string",
                "min": 3,
                "max": 20,
                "default_value": "1234567890",
                "default_random": 0.1
            }
        }
    ];

    // Локальная конфигурация индексных значений
    _testIndexConfig = [
        {
            fieldName: "f1",
            dateName: "dt",
            indexTypeName: "test_type_1",
            indexType: 1,
            indexValue: 1
        },
        {
            fieldName: "f2",
            dateName: "dt",
            indexTypeName: "test_type_2",
            indexType: 2,
            indexValue: 2
        },
        {
            fieldName: "f3",
            dateName: "dt",
            indexTypeName: "test_type_3",
            indexType: 3,
            indexValue: 1
        }
    ];
    constructor() {
        this.logger = Logger.fromEnv('LOG_LEVEL', 'DEBUG');
        this.provider = new MongoProvider(
            config.database.connectionString,
            'factControllerTestDB',
            config.database.options,
            null,
            config.facts.includeFactDataToIndex,
            config.facts.lookupFacts,
            config.facts.indexBulkUpdate
        );


        this.controller = new FactService(this.provider, this._testFieldConfig, this._testIndexConfig, 500, config.facts.includeFactDataToIndex, config.facts.maxDepthLimit);
        this.testResults = {
            passed: 0,
            failed: 0,
            errors: []
        };
    }

    /**
     * Запуск всех тестов
     */
    async runAllTests() {
        this.logger.debug('=== Тестирование всех методов FactController (7 тестов) ===\n');

        try {
            // Подключение к базе данных
            await this.provider.connect();
            this.logger.debug('✓ Подключен к базе данных для тестирования FactController\n');

            // Запуск тестов
            await this.testRunBasic('1. Тест базового выполнения метода run...');
            await this.testRunWithExistingFacts('2. Тест выполнения с существующими фактами...');
            await this.testProcessMessage('3. Тест метода processMessage...');
            await this.testProcessMessageWithCounters('4. Тест метода processMessageWithCounters...');
            await this.testRunMultipleTimes('5. Тест многократного выполнения...');
            await this.testRunWithEmptyDatabase('6. Тест выполнения с пустой базой данных...');
            await this.testRunErrorHandling('7. Тест обработки ошибок...');

            await this.testRunProcessMessage('8. Тест обработки реального сообщения...');

            // Отключение от базы данных
            await this.provider.disconnect();
            this.logger.debug('✓ Отключен от базы данных\n');

        } catch (error) {
            this.logger.error('✗ Ошибка при выполнении тестов FactController:', error.message);
            await this.provider.disconnect();
        }

        // Вывод результатов
        this.printResults();
    }

    /**
     * Тест базового выполнения метода run
     */
    async testRunBasic(title) {
        this.logger.debug(title);

        try {
            // Очищаем базу данных перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Выполняем метод run
            const result = await this.controller.run();

            // Проверяем, что результат содержит ожидаемые поля
            if (!result || typeof result !== 'object') {
                throw new Error('Метод run должен возвращать объект');
            }

            if (!result.fact) {
                throw new Error('Результат должен содержать поле fact');
            }

            if (!result.relevantFacts) {
                throw new Error('Результат должен содержать поле relevantFacts');
            }

            if (!result.saveFactResult) {
                throw new Error('Результат должен содержать поле saveFactResult');
            }

            if (!result.saveIndexResult) {
                throw new Error('Результат должен содержать поле saveIndexResult');
            }

            // Проверяем структуру сгенерированного факта
            const fact = result.fact;
            const requiredFields = ['_id', 't', 'c', 'd'];
            for (const field of requiredFields) {
                if (!(field in fact)) {
                    throw new Error(`Сгенерированный факт должен содержать поле ${field}`);
                }
            }

            // Проверяем, что relevantFacts является массивом
            if (!Array.isArray(result.relevantFacts)) {
                throw new Error('relevantFacts должен быть массивом');
            }

            // Проверяем, что факт был сохранен в базе данных
            const savedFacts = await this.provider.findFacts({ _id: fact._id });
            if (savedFacts.length === 0) {
                throw new Error('Факт должен быть сохранен в базе данных');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunBasic', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест выполнения метода run с существующими фактами
     */
    async testRunWithExistingFacts(title) {
        this.logger.debug(title);

        try {
            // Создаем несколько тестовых фактов с общими значениями полей
            const testFacts = [
                {
                    _id: 'test-fact-001',
                    t: 1,
                    c: new Date(),
                    d: {
                        a: 100,
                        dt: new Date(),
                        f1: 'shared-value',
                        f2: 'unique1'
                    }
                },
                {
                    _id: 'test-fact-002',
                    t: 1,
                    c: new Date(),
                    d: {
                        a: 200,
                        dt: new Date(),
                        f1: 'shared-value',
                        f2: 'unique2'
                    }
                }
            ];

            // Сохраняем тестовые факты
            for (const fact of testFacts) {
                await this.provider.saveFact(fact);
                const indexValues = this.controller.factIndexer.index(fact);
                await this.provider.saveFactIndexList(indexValues);
            }

            // Создаем факт с теми же значениями полей, что и тестовые факты
            const testFact = {
                _id: 'test-fact-query',
                t: 1,
                c: new Date(),
                d: {
                    a: 300,
                    dt: new Date(),
                    f1: 'shared-value',
                    f2: 'unique3'
                }
            };

            // Тестируем getRelevantFacts напрямую
            const testFactIndexValues = this.controller.factIndexer.index(testFact);
            const testHashValuesForSearch = this.controller.factIndexer.getHashValuesForSearch(testFactIndexValues);
            const excludedFact = testFacts[1];
            const factsResult = await this.provider.getRelevantFacts(testHashValuesForSearch, excludedFact);
            const relevantFacts = factsResult.result;

            // Проверяем, что relevantFacts содержит существующие факты
            if (relevantFacts.length === 0) {
                throw new Error('relevantFacts должен содержать существующие факты');
            }

            // Проверяем, что найденные факты содержат ожидаемые ID
            // relevantFacts содержит объекты с полем fact, поэтому извлекаем факты
            const foundIds = relevantFacts.map(f => f._id).filter(id => id); // Фильтруем пустые ID
            const expectedIds = testFacts.map(f => f._id);
            const hasExpectedFact = expectedIds.some(id => foundIds.includes(id));

            if (!hasExpectedFact) {
                this.logger.debug(`Найденные ID: ${foundIds.join(', ')}`);
                this.logger.debug(`Ожидаемые ID: ${expectedIds.join(', ')}`);
                this.logger.debug(`Полные найденные факты:`, JSON.stringify(relevantFacts, null, 2));
                throw new Error('relevantFacts должен содержать хотя бы один из тестовых фактов');
            }

            // Теперь выполняем обычный метод run
            const result = await this.controller.run();

            // Проверяем, что новый факт был сохранен
            const newFact = result.fact;
            const savedFacts = await this.provider.findFacts({ i: newFact.i });
            if (savedFacts.length === 0) {
                throw new Error('Новый факт должен быть сохранен в базе данных');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunWithExistingFacts', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест многократного выполнения метода run
     */
    async testRunMultipleTimes(title) {
        this.logger.debug(title);

        try {
            const runCount = 3;
            const results = [];

            // Выполняем метод run несколько раз
            for (let i = 0; i < runCount; i++) {
                const result = await this.controller.run();
                results.push(result);
            }

            // Проверяем, что все результаты корректны
            for (let i = 0; i < results.length; i++) {
                const result = results[i];
                if (!result.fact || !result.relevantFacts || !result.saveFactResult || !result.saveIndexResult) {
                    throw new Error(`Результат ${i + 1} должен содержать все обязательные поля: ${JSON.stringify(result)}`);
                }
            }

            // Проверяем, что все факты уникальны
            const factIds = results.map(r => r.fact._id);
            const uniqueIds = new Set(factIds);
            if (uniqueIds.size !== factIds.length) {
                throw new Error('Все сгенерированные факты должны иметь уникальные ID');
            }

            // Проверяем, что все факты сохранены в базе данных
            for (const result of results) {
                const savedFacts = await this.provider.findFacts({ _id: result.fact._id });
                if (savedFacts.length === 0) {
                    throw new Error(`Факт ${result.fact.i} должен быть сохранен в базе данных`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunMultipleTimes', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест выполнения метода run с пустой базой данных
     */
    async testRunWithEmptyDatabase(title) {
        this.logger.debug(title);

        try {
            // Очищаем базу данных
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Выполняем метод run
            const result = await this.controller.run();

            // Проверяем, что relevantFacts пустой массив
            if (!Array.isArray(result.relevantFacts)) {
                throw new Error('relevantFacts должен быть массивом');
            }

            if (result.relevantFacts.length !== 0) {
                throw new Error('relevantFacts должен быть пустым массивом при пустой базе данных');
            }

            // Проверяем, что факт был сохранен
            const savedFacts = await this.provider.findFacts({ i: result.fact.i });
            if (savedFacts.length === 0) {
                throw new Error('Факт должен быть сохранен даже при пустой базе данных');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunWithEmptyDatabase', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест 5: Метод processMessage
     */
    async testProcessMessage(title) {
        this.logger.debug(title);

        try {
            // Очищаем базу данных перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Создаем тестовое сообщение
            const testMessage = {
                t: 1,
                d: {
                    dt: new Date('2024-01-01T00:00:00.000Z'),
                    a: 100,
                    f1: 'test-fact-001',
                    f2: 'value1'
                }
            };

            // Вызываем метод processMessage
            const result = await this.controller.processMessage(testMessage);

            // Проверяем структуру результата
            if (!result || typeof result !== 'object') {
                throw new Error('processMessage должен возвращать объект');
            }

            // Проверяем наличие обязательных полей в результате
            const requiredFields = ['fact', 'relevantFacts', 'saveFactResult', 'saveIndexResult'];
            for (const field of requiredFields) {
                if (!(field in result)) {
                    throw new Error(`Отсутствует обязательное поле: ${field}`);
                }
            }

            // Проверяем структуру факта
            if (!result.fact || typeof result.fact !== 'object') {
                throw new Error('Поле fact должно быть объектом');
            }

            // Проверяем обязательные поля факта
            const factRequiredFields = ['_id', 't', 'c', 'd'];
            for (const field of factRequiredFields) {
                if (!(field in result.fact)) {
                    throw new Error(`Отсутствует обязательное поле в факте: ${field}`);
                }
            }

            // Проверяем, что relevantFacts является массивом
            if (!result.relevantFacts || !Array.isArray(result.relevantFacts)) {
                throw new Error('Поле relevantFacts должно быть массивом');
            }

            // Проверяем, что saveFactResult содержит информацию о сохранении
            if (!result.saveFactResult || typeof result.saveFactResult !== 'object') {
                throw new Error('Поле saveFactResult должно быть объектом');
            }

            // Проверяем, что saveIndexResult содержит информацию о сохранении индексов
            if (!result.saveIndexResult || typeof result.saveIndexResult !== 'object') {
                throw new Error('Поле saveIndexResult должно быть объектом');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testProcessMessage', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест 6: Метод processMessageWithCounters
     */
    async testProcessMessageWithCounters(title) {
        this.logger.debug(title);

        try {
            // Очищаем базу данных перед тестом
            await this.provider.clearFactsCollection();
            await this.provider.clearFactIndexCollection();

            // Создаем тестовое сообщение
            const testMessage = {
                t: 1,
                d: {
                    dt: new Date('2024-01-01T00:00:00.000Z'),
                    a: 100,
                    f1: 'test-fact-001',
                    f2: 'value1'
                }
            };

            // Вызываем метод processMessageWithCounters
            const result = await this.controller.processMessageWithCounters(testMessage);

            // Проверяем структуру результата
            if (!result || typeof result !== 'object') {
                throw new Error('processMessageWithCounters должен возвращать объект');
            }

            // Проверяем наличие обязательных полей в результате
            const requiredFields = ['fact', 'counters', 'saveFactResult', 'saveIndexResult'];
            for (const field of requiredFields) {
                if (!(field in result)) {
                    throw new Error(`Отсутствует обязательное поле: ${field}`);
                }
            }

            // Проверяем структуру факта
            if (!result.fact || typeof result.fact !== 'object') {
                throw new Error('Поле fact должно быть объектом');
            }

            // Проверяем обязательные поля факта
            const factRequiredFields = ['_id', 't', 'c', 'd'];
            for (const field of factRequiredFields) {
                if (!(field in result.fact)) {
                    throw new Error(`Отсутствует обязательное поле в факте: ${field}`);
                }
            }

            // Проверяем, что counters является массивом
            if (!result.counters ||  typeof result.counters !== 'object') {
                throw new Error('Поле counters должно быть объектом');
            }

            // Проверяем, что saveFactResult содержит информацию о сохранении
            if (!result.saveFactResult || typeof result.saveFactResult !== 'object') {
                throw new Error('Поле saveFactResult должно быть объектом');
            }

            // Проверяем, что saveIndexResult содержит информацию о сохранении индексов
            if (!result.saveIndexResult || typeof result.saveIndexResult !== 'object') {
                throw new Error('Поле saveIndexResult должно быть объектом');
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testProcessMessageWithCounters', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Тест 6: Обработка ошибок в методе run
     */
    async testRunErrorHandling(title) {
        this.logger.debug(title);

        try {
            // Создаем контроллер с невалидным провайдером
            const invalidProvider = {
                getRelevantFacts: () => { throw new Error('Тестовая ошибка getRelevantFacts'); },
                getRelevantFactCounters: () => { throw new Error('Тестовая ошибка getRelevantFactCounters'); },
                saveFact: () => { throw new Error('Тестовая ошибка saveFact'); },
                saveFactIndexList: () => { throw new Error('Тестовая ошибка saveFactIndexList'); }
            };

            const testController = new FactService(invalidProvider, this._testFieldConfig, this._testIndexConfig, 500, config.facts.includeFactDataToIndex, config.facts.maxDepthLimit);

            // Пытаемся выполнить метод run
            try {
                await testController.run();
                throw new Error('Метод run должен выбрасывать ошибку при проблемах с провайдером');
            } catch (error) {
                // Ожидаем, что ошибка будет выброшена
                if (!error.message.includes('Тестовая ошибка')) {
                    throw new Error(`Ожидалась тестовая ошибка, получена: ${error.message}`);
                }
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunErrorHandling', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
      * Тест 7: Обработка ошибок в методе processMessage
      */
    async testRunProcessMessage(title) {
        this.logger.debug(title);

        try {
            const realController = new FactService(this.provider, config.facts.fieldConfigPath, config.facts.indexConfigPath, config.facts.targetSize, config.facts.includeFactDataToIndex, config.facts.maxDepthLimit);
            const message = {
                "t": 1,
                "d": {
                    "MessageId": "b9dfef143335323448e67f44",
                    "id": "b9dfef143335323448e67f44",
                    "acc_identification_flag": 6268268,
                    "acc_prepaid": false,
                    "agreement": 5041469,
                    "custom_auth_limit": 859138,
                    "dst_client_id": "11111111111111111111",
                    "fid_md": 2310595,
                    "from_ros": 2905896,
                    "grade": 2650440,
                    "inn": "TXZzGzSh",
                    "INN_recepient": "VAQSeHtsVigH",
                    "insurance_activity": 9573280,
                    "ip_address": "GNQgmvnMpiUyxr",
                    "kir_flg": 4819562,
                    "md_agreement_rosbank_flag": 1000,
                    "md_fid01": 5565295,
                    "md_fid01.2": 6896493,
                    "md_fid02": 6064396,
                    "md_fid02.2": 7076603,
                    "md_fid03": 1000,
                    "md_fid04": 1000,
                    "md_fid08.1": 7733285,
                    "md_fid08.2": 1076386,
                    "md_fid08.3": 3899740,
                    "md_fid09": 6142205,
                    "md_fid10": 1000,
                    "md_fid11": 3976599,
                    "md_fid15": 603956,
                    "md_fid15.2": 7102240,
                    "md_fid16": 8643628,
                    "md_fid16.2": 4360033,
                    "md_rosbank_userAgent": "KZWsPlfQVjPyORbBDwyYxX",
                    "monthly_limit": 7427157,
                    "msgMode": "GMymyrafEqGLZFTcdDb",
                    "organization_id": "bXjrtPERrjEd",
                    "p_dstCardNumber": "IttFD",
                    "p_dstClientId": "MZagaxbeIf",
                    "p_dstOrganisationID": "gLKSTshxppWRtdy",
                    "pan": "XSjPwSsCYQiHitFwiwDFlYFgi",
                    "PAN": "yISacfuHuQL",
                    "pe_aml_status": 7375493,
                    "pe_birthdate_dst": "2024-12-07T11:51:13.717Z",
                    "pe_child_flag": 3428079,
                    "pe_drop_flag": 558231,
                    "pe_dst_aml_status": 3670127,
                    "pe_parent_flag": 9302796,
                    "pe_phone_dst": "ZOVKGWK",
                    "pe_pm_client_id": "ifIl",
                    "pe_pm_flag_num": 3238726,
                    "pe_rosbank_private_flag": 7125692,
                    "pe_vip_flag_2_phone": 5181013,
                    "ph_segment": 9114887,
                    "phone": "YgKTFlAPVlqM",
                    "receiver_acc_and_bik": "YtaWLbVtGlLMvZiD",
                    "rosbank_risk_flag1": 461762,
                    "rosbank_risk_flag2": 7425118,
                    "rosbank_risk_flag3": 1000,
                    "s_client_id": "zjGpcWcIxLrbYSrGxN",
                    "s_organization_id": "oIHKySHianxOTuWJei",
                    "subscription": 5318922,
                    "subscription_date": "2024-03-03T15:23:42.090Z",
                    "sum_credlimit": 369334,
                    "timestamp": "2024-06-21T22:59:54.182Z",
                    "vul_flg": false
                }
            };

            const processResult = await realController.processMessage(message);
            if (!processResult || typeof processResult !== 'object') {
                throw new Error('processMessage должен возвращать объект: ' + processResult);
            }
            if (!processResult.saveFactResult || typeof processResult.saveFactResult !== 'object') {
                throw new Error('Поле saveFactResult должно быть объектом: ' + processResult.saveFactResult);
            }
            if (!processResult.saveIndexResult || typeof processResult.saveIndexResult !== 'object') {
                throw new Error('Поле saveIndexResult должно быть объектом: ' + processResult.saveIndexResult);
            }
            if (!processResult.saveFactResult) {
                throw new Error('Ошибка при сохранении факта: ' + JSON.stringify(processResult.saveFactResult));
            }
            if (!processResult.saveIndexResult) {
                throw new Error('Ошибка при сохранении индексных значений: ' + JSON.stringify(processResult.saveIndexResult));
            }

            this.testResults.passed++;
            this.logger.debug('   ✓ Успешно');
        } catch (error) {
            this.testResults.failed++;
            this.testResults.errors.push({ test: 'testRunErrorHandling', error: error.message });
            this.logger.error(`   ✗ Ошибка: ${error.message}`);
        }
    }

    /**
     * Вывод результатов тестирования
     */
    printResults() {
        this.logger.info('\n=== Результаты тестирования FactController ===');
        this.logger.info(`Пройдено: ${this.testResults.passed}`);
        this.logger.info(`Провалено: ${this.testResults.failed}`);

        if (this.testResults.errors.length > 0) {
            this.logger.error('\nОшибки:');
            for (const error of this.testResults.errors) {
                this.logger.error(`  - ${error.test}: ${error.error}`);
            }
        }

        const totalTests = this.testResults.passed + this.testResults.failed;
        const successRate = totalTests > 0 ? (this.testResults.passed / totalTests * 100).toFixed(1) : 0;
        this.logger.info(`\nПроцент успеха: ${successRate}%`);
    }
}

// Запуск тестов, если файл выполняется напрямую
if (require.main === module) {
    const test = new FactControllerTest();
    test.runAllTests().catch(console.error);
}

module.exports = FactControllerTest;