const cluster = require('cluster');
const os = require('os');
const http = require('http');
const Logger = require('../logger');
const ClusterMetricsAggregator = require('../monitoring/clusterMetricsAggregator');

// Загружаем переменные окружения
const dotenv = require('dotenv');
dotenv.config();

const logger = Logger.fromEnv('LOG_LEVEL', 'INFO');

// Количество CPU ядер для создания воркеров
const numCPUs = process.env.CLUSTER_WORKERS || os.cpus().length;
// Общий порт для сбора метрик
const metricsPort = parseInt(process.env.METRICS_PORT) || 12081;

if (cluster.isMaster) {
    logger.info(`🚀 Master процесс ${process.pid} запущен`);
    logger.info(`⚙️  Создаю ${numCPUs} воркеров для обработки запросов`);
    
    // Инициализируем агрегатор метрик
    logger.info(`🔧 Инициализирую агрегатор метрик...`);
    const metricsAggregator = new ClusterMetricsAggregator();
    logger.info(`✅ Агрегатор метрик инициализирован`);
    
    // Создаем HTTP сервер для метрик на отдельном порту
    const metricsServer = http.createServer(async (req, res) => {
        if (req.url === '/metrics') {
            try {
                // Получаем объединенные метрики от всех worker'ов
                const combinedMetrics = await metricsAggregator.getCombinedMetrics();
                res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
                res.end(combinedMetrics);
            } catch (error) {
                logger.error('Ошибка при получении агрегированных метрик:', error);
                logger.error('Stack trace:', error.stack);
                res.statusCode = 500;
                res.end(`Internal Server Error: ${error.message}`);
            }
        } else {
            res.statusCode = 404;
            res.end('Not Found');
        }
    });
    
    metricsServer.listen(metricsPort, () => {
        logger.info(`📊 Сервер метрик запущен на порту ${metricsPort}`);
        logger.info(`📈 Метрики доступны по адресу: http://localhost:${metricsPort}/metrics`);
    });
    
    // Выводим информацию о разрешенных типах сообщений
    const config = require('../config');
    logger.info(`📨 Обрабатываемые типы сообщений:`);
    if (config.messageTypes.allowedTypes && config.messageTypes.allowedTypes.length > 0) {
        logger.info(`   - Разрешенные типы: ${config.messageTypes.allowedTypes.join(', ')}`);
        logger.info(`   - Всего типов: ${config.messageTypes.allowedTypes.length}`);
    } else {
        logger.info(`   - Все типы сообщений разрешены (фильтрация отключена)`);
    }

    // Создаем воркеры
    for (let i = 0; i < numCPUs; i++) {
        try {
            logger.info(`🔧 Создаю воркер ${i + 1}/${numCPUs}...`);
            const worker = cluster.fork();
            logger.info(`👷 Воркер ${worker.process.pid} создан`);
            
            // Добавляем обработчики ошибок для каждого воркера
            worker.on('error', (err) => {
                logger.error(`❌ Ошибка в воркере ${worker.process.pid}:`, err.message);
                logger.error(`📋 Детали ошибки:`, err);
            });
            
            worker.on('disconnect', () => {
                logger.warn(`⚠️  Воркер ${worker.process.pid} отключился`);
            });
            
            // Добавляем обработчик сообщений от воркера
            worker.on('message', (msg) => {
                if (msg.type === 'worker-ready') {
                    logger.info(`✅ Воркер ${worker.process.pid} готов к работе`);
                } else if (msg.type === 'worker-error') {
                    logger.error(`❌ Воркер ${worker.process.pid} сообщает об ошибке:`, msg.error);
                } else if (msg.type === 'worker-metrics') {
                    // Обновляем метрики от worker'а
                    metricsAggregator.updateWorkerMetrics(msg.workerId, msg.metricsData);
                }
            });
            
        } catch (error) {
            logger.error(`❌ Не удалось создать воркер ${i + 1}:`, error.message);
            logger.error(`📋 Проверьте системные ресурсы и конфигурацию`);
        }
    }

    // Обработка завершения воркеров
    cluster.on('exit', (worker, code, signal) => {
        const workerInfo = `Воркер ${worker.process.pid}`;
        
        // Удаляем метрики завершенного worker'а
        metricsAggregator.removeWorkerMetrics(`worker-${worker.process.pid}`);
        
        if (signal) {
            logger.warn(`⚠️  ${workerInfo} завершен сигналом ${signal}`);
            logger.info(`📋 Возможные причины сигнала ${signal}:`);
            logger.info(`   - SIGTERM: Корректное завершение процесса`);
            logger.info(`   - SIGINT: Прерывание пользователем (Ctrl+C)`);
            logger.info(`   - SIGKILL: Принудительное завершение системы`);
        } else if (code !== 0) {
            logger.error(`❌ ${workerInfo} завершен с кодом ошибки ${code}`);
            logger.error(`📋 Диагностика ошибки ${code}:`);
            
            // Расшифровка кодов ошибок Windows
            switch (code) {
                case 3221225786:
                    logger.error(`   🔍 Код 3221225786 (0xC0000005): Нарушение доступа к памяти`);
                    logger.error(`   💡 Возможные причины:`);
                    logger.error(`      - Проблемы с подключением к MongoDB`);
                    logger.error(`      - Ошибки в конфигурационных файлах`);
                    logger.error(`      - Недостаток памяти или ресурсов`);
                    logger.error(`      - Конфликт портов или сетевых соединений`);
                    break;
                case 3221226505:
                    logger.error(`   🔍 Код 3221226505 (0xC0000135): Модуль не найден`);
                    logger.error(`   💡 Возможные причины:`);
                    logger.error(`      - Отсутствуют зависимости Node.js`);
                    logger.error(`      - Повреждены файлы node_modules`);
                    logger.error(`      - Проблемы с путями к модулям`);
                    break;
                case 3221225477:
                    logger.error(`   🔍 Код 3221225477 (0xC0000005): Нарушение доступа`);
                    logger.error(`   💡 Возможные причины:`);
                    logger.error(`      - Проблемы с правами доступа к файлам`);
                    logger.error(`      - Блокировка файлов антивирусом`);
                    logger.error(`      - Конфликт с другими процессами`);
                    break;
                default:
                    logger.error(`   🔍 Неизвестный код ошибки: ${code}`);
                    logger.error(`   💡 Проверьте логи воркера для детальной информации`);
            }
            
            logger.error(`🔧 Рекомендации по устранению:`);
            logger.error(`   1. Проверьте подключение к MongoDB: mongodb://admin:admin@localhost:27020`);
            logger.error(`   2. Убедитесь, что конфигурационные файлы существуют и корректны`);
            logger.error(`   3. Проверьте доступность порта 3000: netstat -an | findstr :3000`);
            logger.error(`   4. Запустите один воркер для диагностики: npm run start:worker`);
        } else {
            logger.info(`✅ ${workerInfo} завершен успешно`);
        }

        // Перезапускаем воркер если он завершился неожиданно (только если не идет завершение)
        if (!worker.exitedAfterDisconnect && !isShuttingDown) {
            logger.info(`🔄 Перезапускаю воркер через 2 секунды...`);
            setTimeout(() => {
                // Проверяем еще раз, не началось ли завершение
                if (isShuttingDown) {
                    logger.info(`⚠️  Завершение в процессе, пропускаю перезапуск воркера`);
                    return;
                }
                
                try {
                    const newWorker = cluster.fork();
                    logger.info(`👷 Новый воркер ${newWorker.process.pid} создан`);
                    
                    // Добавляем обработчик ошибок для нового воркера
                    newWorker.on('error', (err) => {
                        logger.error(`❌ Ошибка в новом воркере ${newWorker.process.pid}:`, err.message);
                        logger.error(`📋 Детали ошибки:`, err);
                    });
                } catch (error) {
                    logger.error(`❌ Не удалось создать новый воркер:`, error.message);
                    logger.error(`📋 Проверьте системные ресурсы и конфигурацию`);
                }
            }, 2000);
        } else if (isShuttingDown) {
            logger.info(`✅ Воркер ${worker.process.pid} завершен в рамках graceful shutdown`);
        }
    });

    // Флаг для предотвращения перезапуска воркеров при завершении
    let isShuttingDown = false;

    // Обработка сигналов завершения
    const gracefulShutdown = (signal) => {
        if (isShuttingDown) {
            logger.warn(`⚠️  Уже выполняется завершение, игнорирую сигнал ${signal}`);
            return;
        }
        
        isShuttingDown = true;
        logger.info(`📡 Получен сигнал ${signal}, завершаю все воркеры...`);
        
        // Завершаем агрегатор метрик
        try {
            metricsAggregator.destroy();
            logger.info(`✅ Агрегатор метрик завершен`);
        } catch (error) {
            logger.error(`❌ Ошибка при завершении агрегатора метрик:`, error.message);
        }
        
        // Завершаем сервер метрик
        try {
            metricsServer.close();
            logger.info(`✅ Сервер метрик завершен`);
        } catch (error) {
            logger.error(`❌ Ошибка при завершении сервера метрик:`, error.message);
        }
        
        // Отключаем автоматический перезапуск воркеров
        cluster.removeAllListeners('exit');
        
        // Завершаем все воркеры
        const workerIds = Object.keys(cluster.workers);
        if (workerIds.length === 0) {
            logger.info('✅ Все воркеры уже завершены');
            process.exit(0);
            return;
        }
        
        logger.info(`🔧 Завершаю ${workerIds.length} воркеров...`);
        
        // Отправляем сигнал завершения всем воркерам
        workerIds.forEach(id => {
            const worker = cluster.workers[id];
            if (worker && !worker.isDead()) {
                logger.info(`📡 Отправляю сигнал завершения воркеру ${worker.process.pid}`);
                worker.kill(signal);
            }
        });
        
        // Ждем завершения всех воркеров с таймаутом
        let completedWorkers = 0;
        const checkCompletion = () => {
            const activeWorkers = Object.keys(cluster.workers).filter(id => !cluster.workers[id].isDead());
            if (activeWorkers.length === 0) {
                logger.info('✅ Все воркеры успешно завершены');
                process.exit(0);
            } else if (completedWorkers >= workerIds.length) {
                logger.warn(`⚠️  Принудительное завершение ${activeWorkers.length} воркеров`);
                activeWorkers.forEach(id => {
                    cluster.workers[id].kill('SIGKILL');
                });
                setTimeout(() => process.exit(0), 1000);
            }
        };
        
        // Слушаем завершение воркеров
        cluster.on('exit', (worker, code, signal) => {
            completedWorkers++;
            logger.info(`✅ Воркер ${worker.process.pid} завершен (код: ${code}, сигнал: ${signal})`);
            checkCompletion();
        });
        
        // Таймаут для принудительного завершения
        setTimeout(() => {
            logger.warn('⚠️  Таймаут завершения, принудительно завершаю оставшиеся воркеры');
            Object.keys(cluster.workers).forEach(id => {
                if (!cluster.workers[id].isDead()) {
                    cluster.workers[id].kill('SIGKILL');
                }
            });
            setTimeout(() => process.exit(1), 2000);
        }, 10000); // 10 секунд таймаут
    };

    // Обработка двойного Ctrl+C для принудительного завершения
    let sigintCount = 0;
    let sigintTimer = null;

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => {
        sigintCount++;
        
        if (sigintCount === 1) {
            logger.info('📡 Получен первый SIGINT (Ctrl+C), начинаю graceful shutdown...');
            logger.info('💡 Для принудительного завершения нажмите Ctrl+C еще раз в течение 3 секунд');
            gracefulShutdown('SIGINT');
            
            // Сбрасываем счетчик через 3 секунды
            sigintTimer = setTimeout(() => {
                sigintCount = 0;
                logger.info('✅ Счетчик принудительного завершения сброшен');
            }, 3000);
        } else if (sigintCount >= 2) {
            logger.warn('⚠️  Получен второй SIGINT (Ctrl+C), принудительное завершение!');
            if (sigintTimer) {
                clearTimeout(sigintTimer);
            }
            
            // Принудительно завершаем все процессы
            Object.keys(cluster.workers).forEach(id => {
                if (!cluster.workers[id].isDead()) {
                    cluster.workers[id].kill('SIGKILL');
                }
            });
            
            setTimeout(() => {
                logger.error('❌ Принудительное завершение');
                process.exit(1);
            }, 1000);
        }
    });

    // Мониторинг производительности
    const monitoringInterval = setInterval(() => {
        if (isShuttingDown) {
            clearInterval(monitoringInterval);
            return;
        }
        
        const workers = Object.keys(cluster.workers).length;
        logger.info(`📊 Активных воркеров: ${workers}`);
    }, 30000); // Каждые 30 секунд

} else {
    // Запускаем воркер
    // Каждый Worker работает в отдельном процессе Node.js
    // и создает свои собственные экземпляры MongoProvider, FactService и CounterProducer
    require('./worker.js');
}
