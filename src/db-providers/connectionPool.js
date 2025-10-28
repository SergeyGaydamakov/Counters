const { MongoClient } = require('mongodb');

/**
 * Создает обработчик событий для мониторинга connection pool
 * @param {Object} client - MongoDB клиент
 * @param {string} clientType - тип клиента ('counter' или 'aggregate')
 * @param {Object} metricsCollector - коллектор метрик (опционально)
 * @param {Object} logger - логгер для записи ошибок (опционально)
 * @returns {Object} объект с методами для управления мониторингом
 */
function connectionPoolStatus(client, clientType = 'unknown', metricsCollector = null, logger = null) {
    let checkedOut = 0;
  
    function onCheckout() {
      checkedOut++;
      
      // Регистрируем в метриках
      if (metricsCollector && typeof metricsCollector.recordConnectionCheckout === 'function') {
        try {
          metricsCollector.recordConnectionCheckout(clientType, checkedOut);
        } catch (error) {
          if (logger) {
            logger.error(`Ошибка при регистрации checkout для ${clientType}: ${error.message}`);
          }
        }
      }
      
      if (logger && logger.debug) {
        logger.debug(`Connection checked out. Total checked out for ${clientType}: ${checkedOut}`);
      }
    }
  
    function onCheckin() {
      checkedOut--;
      
      // Регистрируем в метриках
      if (metricsCollector && typeof metricsCollector.recordConnectionCheckin === 'function') {
        try {
          metricsCollector.recordConnectionCheckin(clientType, checkedOut);
        } catch (error) {
          if (logger) {
            logger.error(`Ошибка при регистрации checkin для ${clientType}: ${error.message}`);
          }
        }
      }
      
      if (logger && logger.debug) {
        logger.debug(`Connection checked in. Total checked out for ${clientType}: ${checkedOut}`);
      }
    }
  
    function onClose() {
      if (client && typeof client.removeListener === 'function') {
        client.removeListener('connectionCheckedOut', onCheckout);
        client.removeListener('connectionCheckedIn', onCheckin);
        client.removeAllListeners('connectionCheckOutStarted');
        client.removeAllListeners('connectionCheckOutFailed');
        client.removeAllListeners('connectionCreated');
        client.removeAllListeners('connectionClosed');
        client.removeAllListeners('connectionPoolCreated');
        client.removeAllListeners('connectionPoolReady');
        client.removeAllListeners('connectionPoolClosed');
        client.removeAllListeners('connectionPoolCleared');
      }
  
      checkedOut = NaN;
      if (logger && logger.debug) {
        logger.debug(`Connection POOL closed for ${clientType}`);
      }
    }
  
    // Decreases count of connections checked out of the pool when connectionCheckedIn event is triggered
    if (client && typeof client.on === 'function') {
      // Основные события для мониторинга checkout
      client.on('connectionCheckedIn', onCheckin);
      client.on('connectionCheckedOut', onCheckout);
      
      // События для анализа производительности
      client.on('connectionCheckOutStarted', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionCheckoutStarted === 'function') {
          try {
            metricsCollector.recordConnectionCheckoutStarted(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации checkout started для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.debug) {
          logger.debug(`Connection checkout started for ${clientType}`);
        }
      });
      
      // События для анализа ошибок
      client.on('connectionCheckOutFailed', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionCheckoutFailed === 'function') {
          try {
            metricsCollector.recordConnectionCheckoutFailed(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации checkout failed для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.warn) {
          logger.warn(`Connection checkout failed for ${clientType}`);
        }
      });
      
      // События для мониторинга жизненного цикла соединений
      client.on('connectionCreated', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionCreated === 'function') {
          try {
            metricsCollector.recordConnectionCreated(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации connection created для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.debug) {
          logger.debug(`Connection created for ${clientType}`);
        }
      });
      
      client.on('connectionClosed', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionClosed === 'function') {
          try {
            metricsCollector.recordConnectionClosed(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации connection closed для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.debug) {
          logger.debug(`Connection closed for ${clientType}`);
        }
      });
      
      // События для мониторинга жизненного цикла пула
      client.on('connectionPoolCreated', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionPoolCreated === 'function') {
          try {
            metricsCollector.recordConnectionPoolCreated(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации pool created для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.debug) {
          logger.debug(`Connection pool created for ${clientType}`);
        }
      });
      
      client.on('connectionPoolReady', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionPoolReady === 'function') {
          try {
            metricsCollector.recordConnectionPoolReady(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации pool ready для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.info) {
          logger.info(`Connection pool ready for ${clientType}`);
        }
      });
      
      client.on('connectionPoolClosed', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionPoolClosed === 'function') {
          try {
            metricsCollector.recordConnectionPoolClosed(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации pool closed для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.debug) {
          logger.debug(`Connection pool closed for ${clientType}`);
        }
      });
      
      client.on('connectionPoolCleared', () => {
        if (metricsCollector && typeof metricsCollector.recordConnectionPoolCleared === 'function') {
          try {
            metricsCollector.recordConnectionPoolCleared(clientType);
          } catch (error) {
            if (logger) {
              logger.error(`Ошибка при регистрации pool cleared для ${clientType}: ${error.message}`);
            }
          }
        }
        if (logger && logger.warn) {
          logger.warn(`Connection pool cleared for ${clientType}`);
        }
      });
  
      // Cleans up event listeners when client is closed
      client.on('close', onClose);
    }
  
    if (logger && logger.debug) {
      logger.debug(`Connection POOL created for ${clientType}`);
    }

    return {
      count: () => checkedOut,
      cleanUp: onClose
    };
}

module.exports = connectionPoolStatus;
