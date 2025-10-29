const fs = require('fs');
const path = require('path');
const net = require('net');
const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

/**
 * Утилиты для диагностики системы и конфигурации
 */
class Diagnostics {
    constructor(logger) {
        this.logger = logger;
    }

    /**
     * Проверяет доступность порта
     */
    async checkPort(port) {
        return new Promise((resolve) => {
            const server = net.createServer();
            server.listen(port, () => {
                server.once('close', () => resolve({ available: true, error: null }));
                server.close();
            });
            server.on('error', (err) => resolve({ available: false, error: err }));
        });
    }

    /**
     * Проверяет существование файла
     */
    checkFile(filePath) {
        try {
            const exists = fs.existsSync(filePath);
            const stats = exists ? fs.statSync(filePath) : null;
            return {
                exists,
                size: stats ? stats.size : 0,
                modified: stats ? stats.mtime : null,
                readable: exists ? fs.accessSync(filePath, fs.constants.R_OK) === undefined : false
            };
        } catch (error) {
            return {
                exists: false,
                error: error.message
            };
        }
    }

    /**
     * Проверяет подключение к MongoDB
     */
    async checkMongoDB(connectionString) {
        try {
            const { MongoClient } = require('mongodb');
            const client = new MongoClient(connectionString, {
                serverSelectionTimeoutMS: 5000,
                connectTimeoutMS: 5000
            });
            
            await client.connect();
            await client.db().admin().ping();
            await client.close();
            
            return { connected: true, error: null };
        } catch (error) {
            return { connected: false, error: error.message };
        }
    }

    /**
     * Проверяет процессы Node.js
     */
    async checkNodeProcesses() {
        try {
            const { stdout } = await execAsync('tasklist /FI "IMAGENAME eq node.exe" /FO CSV');
            const lines = stdout.split('\n').filter(line => line.includes('node.exe'));
            return {
                count: lines.length,
                processes: lines.map(line => {
                    const parts = line.split(',');
                    return {
                        pid: parts[1]?.replace(/"/g, ''),
                        memory: parts[4]?.replace(/"/g, '')
                    };
                })
            };
        } catch (error) {
            return { count: 0, error: error.message };
        }
    }

    /**
     * Проверяет использование портов
     */
    async checkPortUsage(port) {
        try {
            const { stdout } = await execAsync(`netstat -an | findstr :${port}`);
            const lines = stdout.split('\n').filter(line => line.trim());
            return {
                inUse: lines.length > 0,
                connections: lines.map(line => line.trim())
            };
        } catch (error) {
            return { inUse: false, error: error.message };
        }
    }

    /**
     * Выполняет полную диагностику системы
     */
    async runFullDiagnostics(config) {
        this.logger.info('🔍 Запуск полной диагностики системы...');
        
        const results = {
            timestamp: new Date().toISOString(),
            system: {},
            configuration: {},
            services: {},
            recommendations: []
        };

        // Проверка системных ресурсов
        this.logger.info('📊 Проверка системных ресурсов...');
        const memUsage = process.memoryUsage();
        results.system.memory = {
            rss: `${Math.round(memUsage.rss / 1024 / 1024)}MB`,
            heapTotal: `${Math.round(memUsage.heapTotal / 1024 / 1024)}MB`,
            heapUsed: `${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`,
            external: `${Math.round(memUsage.external / 1024 / 1024)}MB`
        };

        // Проверка Node.js процессов
        this.logger.info('🔍 Проверка процессов Node.js...');
        results.system.nodeProcesses = await this.checkNodeProcesses();

        // Проверка конфигурационных файлов
        this.logger.info('📁 Проверка конфигурационных файлов...');
        results.configuration.files = {};
        
        if (config.facts.counterConfigPath) {
            results.configuration.files.counters = this.checkFile(config.facts.counterConfigPath);
        }
        if (config.facts.fieldConfigPath) {
            results.configuration.files.fields = this.checkFile(config.facts.fieldConfigPath);
        }
        if (config.facts.indexConfigPath) {
            results.configuration.files.indexes = this.checkFile(config.facts.indexConfigPath);
        }

        // Проверка портов
        this.logger.info('🌐 Проверка портов...');
        results.services.ports = {};
        
        // Проверка порта приложения
        const appPortCheck = await this.checkPort(config.port);
        results.services.ports.application = {
            port: config.port,
            available: appPortCheck.available,
            error: appPortCheck.error
        };

        // Проверка использования порта
        const portUsage = await this.checkPortUsage(config.port);
        results.services.ports.application.inUse = portUsage.inUse;
        results.services.ports.application.connections = portUsage.connections;

        // Проверка MongoDB
        this.logger.info(`🍃 Проверка подключения к MongoDB ${config.database.connectionString}...`);
        results.services.mongodb = await this.checkMongoDB(config.database.connectionString);

        // Генерация рекомендаций
        this.generateRecommendations(results);

        return results;
    }

    /**
     * Генерирует рекомендации на основе результатов диагностики
     */
    generateRecommendations(results) {
        const recommendations = [];

        // Проверка памяти
        const heapUsedMB = parseInt(results.system.memory.heapUsed);
        if (heapUsedMB > 100) {
            recommendations.push({
                type: 'warning',
                message: 'Высокое потребление памяти',
                details: `Используется ${heapUsedMB}MB heap памяти`,
                action: 'Рассмотрите возможность перезапуска процесса или увеличения лимитов памяти'
            });
        }

        // Проверка процессов Node.js
        if (results.system.nodeProcesses.count > 10) {
            recommendations.push({
                type: 'warning',
                message: 'Много процессов Node.js',
                details: `Запущено ${results.system.nodeProcesses.count} процессов`,
                action: 'Остановите неиспользуемые процессы: taskkill /f /im node.exe'
            });
        }

        // Проверка конфигурационных файлов
        Object.entries(results.configuration.files).forEach(([name, file]) => {
            if (!file.exists) {
                recommendations.push({
                    type: 'error',
                    message: `Конфигурационный файл ${name} не найден`,
                    details: file.error || 'Файл отсутствует',
                    action: 'Проверьте пути к конфигурационным файлам в переменных окружения'
                });
            } else if (!file.readable) {
                recommendations.push({
                    type: 'error',
                    message: `Нет доступа к файлу ${name}`,
                    details: 'Файл существует, но недоступен для чтения',
                    action: 'Проверьте права доступа к файлу'
                });
            }
        });

        // Проверка портов
        if (!results.services.ports.application.available) {
            recommendations.push({
                type: 'error',
                message: `Порт ${results.services.ports.application.port} недоступен`,
                details: results.services.ports.application.error,
                action: 'Проверьте, не используется ли порт другим процессом'
            });
        }

        if (results.services.ports.application.inUse) {
            recommendations.push({
                type: 'warning',
                message: `Порт ${results.services.ports.application.port} уже используется`,
                details: `Найдено ${results.services.ports.application.connections.length} соединений`,
                action: 'Остановите конфликтующие процессы или измените порт'
            });
        }

        // Проверка MongoDB
        if (!results.services.mongodb.connected) {
            recommendations.push({
                type: 'error',
                message: 'MongoDB недоступен',
                details: results.services.mongodb.error,
                action: 'Проверьте запуск MongoDB и правильность строки подключения'
            });
        }

        results.recommendations = recommendations;
    }

    /**
     * Выводит результаты диагностики в лог
     */
    logDiagnostics(results) {
        this.logger.info('📋 Результаты диагностики:');
        this.logger.info(`   Время: ${results.timestamp}`);
        
        // Системные ресурсы
        this.logger.info('📊 Системные ресурсы:');
        this.logger.info(`   Память: ${results.system.memory.heapUsed} heap, ${results.system.memory.rss} RSS`);
        this.logger.info(`   Процессы Node.js: ${results.system.nodeProcesses.count}`);
        
        // Конфигурация
        this.logger.info('📁 Конфигурационные файлы:');
        Object.entries(results.configuration.files).forEach(([name, file]) => {
            if (file.exists) {
                this.logger.info(`   ✅ ${name}: ${file.size} байт`);
            } else {
                this.logger.info(`   ❌ ${name}: не найден`);
            }
        });
        
        // Сервисы
        this.logger.info('🌐 Сервисы:');
        this.logger.info(`   Порт ${results.services.ports.application.port}: ${results.services.ports.application.available ? '✅ доступен' : '❌ недоступен'}`);
        this.logger.info(`   MongoDB: ${results.services.mongodb.connected ? '✅ подключен' : '❌ недоступен'}`);
        
        // Рекомендации
        if (results.recommendations.length > 0) {
            this.logger.info('🔧 Рекомендации:');
            results.recommendations.forEach((rec, index) => {
                const icon = rec.type === 'error' ? '❌' : '⚠️';
                this.logger.info(`   ${icon} ${rec.message}`);
                this.logger.info(`      ${rec.details}`);
                this.logger.info(`      Действие: ${rec.action}`);
            });
        } else {
            this.logger.info('✅ Все проверки пройдены успешно');
        }
    }
}

module.exports = Diagnostics;
