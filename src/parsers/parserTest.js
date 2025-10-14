const CsvToCountersCfgParser = require('./csvToCountersCfgParser');
const fs = require('fs');
const path = require('path');

/**
 * Тестовый скрипт для проверки парсера CSV в CountersCfg
 */
class ParserTest {
    constructor() {
        this.parser = new CsvToCountersCfgParser();
    }

    /**
     * Тестирует парсер на нескольких примерах
     */
    async testParser() {
        console.log('=== Тестирование парсера CSV в CountersCfg ===\n');

        // Тест 1: Простые условия
        console.log('Тест 1: Простые условия');
        const test1 = this.parser.parseConditions('"(MessageTypeID = 61; 50); (msgMode = CI)"', 'evaluation', 1);
        console.log('Результат:', JSON.stringify(test1, null, 2));
        console.log('');

        // Тест 2: Условия с подстроками
        console.log('Тест 2: Условия с подстроками');
        const test2 = this.parser.parseConditions('"(rules =*= atm88.1); (s_origin ¬= Card)"', 'evaluation', 2);
        console.log('Результат:', JSON.stringify(test2, null, 2));
        console.log('');

        // Тест 3: Условия с null значениями
        console.log('Тест 3: Условия с null значениями');
        const test3 = this.parser.parseConditions('"(s_client_id = ∅); (PAN ≠ ∅)"', 'computation', 3);
        console.log('Результат:', JSON.stringify(test3, null, 2));
        console.log('');

        // Тест 4: Условия с ссылками на поля
        console.log('Тест 4: Условия с ссылками на поля');
        const test4 = this.parser.parseConditions('"(PAN ≠ [p_basicFieldKey]); (Amount = {Amount})"', 'evaluation', 4);
        console.log('Результат:', JSON.stringify(test4, null, 2));
        console.log('');

        // Тест 5: Атрибуты
        console.log('Тест 5: Атрибуты');
        const test5 = this.parser.parseAttributes('1_atm_CI_after_atm88_1h (frequency)', 5);
        console.log('Результат:', JSON.stringify(test5, null, 2));
        console.log('');

        // Тест 6: Атрибуты с суммой
        console.log('Тест 6: Атрибуты с суммой');
        const test6 = this.parser.parseAttributes('1_atm_CI_auth_by_TPAN_20m_sum (total amount)', 6);
        console.log('Результат:', JSON.stringify(test6, null, 2));
        console.log('');

        // Тест 7: Атрибуты с уникальными значениями
        console.log('Тест 7: Атрибуты с уникальными значениями');
        const test7 = this.parser.parseAttributes('1_atm_CI_autt_by_TPAN_20m_dst (distinct values number)', 7);
        console.log('Результат:', JSON.stringify(test7, null, 2));
        console.log('');

        console.log('=== Тестирование завершено ===');
    }

    /**
     * Проверяет корректность структуры JSON
     */
    validateJsonStructure(jsonPath) {
        try {
            const content = fs.readFileSync(jsonPath, 'utf8');
            const data = JSON.parse(content);
            
            console.log('=== Валидация структуры JSON ===');
            console.log(`Общее количество счетчиков: ${data.counters.length}`);
            console.log(`Ошибок при парсинге: ${data.metadata.errors.length}`);
            
            // Проверяем первые 5 счетчиков
            for (let i = 0; i < Math.min(5, data.counters.length); i++) {
                const counter = data.counters[i];
                console.log(`\nСчетчик ${i + 1}: ${counter.name}`);
                console.log(`- Комментарий: ${counter.comment.substring(0, 50)}...`);
                console.log(`- Индекс: ${counter.indexTypeName}`);
                console.log(`- Computation условий: ${Object.keys(counter.computationConditions).length}`);
                console.log(`- Evaluation условий: ${counter.evaluationConditions ? Object.keys(counter.evaluationConditions).length : 0}`);
                console.log(`- Атрибутов: ${Object.keys(counter.attributes).length}`);
            }
            
            return true;
        } catch (error) {
            console.error('Ошибка валидации JSON:', error.message);
            return false;
        }
    }

    /**
     * Запускает полное тестирование
     */
    async runFullTest() {
        try {
            await this.testParser();
            
            const jsonPath = path.join(__dirname, '../../countersCfg.json');
            if (fs.existsSync(jsonPath)) {
                this.validateJsonStructure(jsonPath);
            } else {
                console.log('Файл countersCfg.json не найден. Запустите парсер сначала.');
            }
            
        } catch (error) {
            console.error('Ошибка тестирования:', error);
        }
    }
}

// Запуск тестов
if (require.main === module) {
    const tester = new ParserTest();
    tester.runFullTest();
}

module.exports = ParserTest;
