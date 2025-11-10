#!/usr/bin/env node

/**
 * Скрипт диагностики системы для Counter Service
 * Запуск: node src/utils/runDiagnostics.js
 */

const Logger = require('../logger');
const Diagnostics = require('./diagnostics');
const config = require('../config');

// Загружаем переменные окружения
const dotenv = require('dotenv');
dotenv.config();

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

async function main() {
    try {
        logger.info('🔍 Запуск диагностики Counter Service...');
        logger.info('='.repeat(60));
        
        const diagnostics = new Diagnostics(logger);
        const results = await diagnostics.runFullDiagnostics(config);
        
        logger.info('='.repeat(60));
        diagnostics.logDiagnostics(results);
        
        // Выводим детальный отчет
        logger.info('\n📊 Детальный отчет:');
        logger.info(JSON.stringify(results, null, 2));
        
        // Определяем статус системы
        const hasErrors = results.recommendations.some(rec => rec.type === 'error');
        const hasWarnings = results.recommendations.some(rec => rec.type === 'warning');
        
        if (hasErrors) {
            logger.error('\n❌ Система не готова к запуску. Исправьте критические ошибки.');
            process.exit(1);
        } else if (hasWarnings) {
            logger.warn('\n⚠️  Система готова к запуску, но есть предупреждения.');
            process.exit(0);
        } else {
            logger.info('\n✅ Система полностью готова к запуску.');
            process.exit(0);
        }
        
    } catch (error) {
        logger.error('❌ Ошибка при выполнении диагностики:', error.message);
        logger.error('📋 Детали:', error);
        process.exit(1);
    }
}

// Запуск если файл выполняется напрямую
if (require.main === module) {
    main();
}

module.exports = { main };
