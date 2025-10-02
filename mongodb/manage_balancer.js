// Скрипт для управления балансировщиком шардирования
// Использование: mongosh --file manage_balancer.js [start|stop|status|enable|disable]

print("=== Управление балансировщиком шардирования ===");

// Получение аргумента команды
const action = (typeof args !== 'undefined' && args[0]) || 'status';

print(`Действие: ${action}`);

// Функция для выполнения команд с обработкой ошибок
function executeCommand(command, description) {
    try {
        print(`Выполнение: ${description}`);
        const result = db.runCommand(command);
        if (result.ok === 1) {
            print(`✓ Успешно: ${description}`);
            return { success: true, result: result };
        } else {
            print(`✗ Ошибка: ${description} - ${result.errmsg || 'Неизвестная ошибка'}`);
            return { success: false, error: result.errmsg || 'Неизвестная ошибка' };
        }
    } catch (error) {
        print(`✗ Исключение: ${description} - ${error.message}`);
        return { success: false, error: error.message };
    }
}

// Выполнение действия в зависимости от аргумента
switch (action) {
    case 'start':
        print("\nЗапуск балансировщика...");
        const startResult = executeCommand(
            { startBalancer: 1 },
            "Запуск балансировщика"
        );
        
        if (startResult.success) {
            print("✓ Балансировщик запущен успешно");
        } else {
            print("✗ Ошибка запуска балансировщика");
        }
        break;
        
    case 'stop':
        print("\nОстановка балансировщика...");
        const stopResult = executeCommand(
            { stopBalancer: 1 },
            "Остановка балансировщика"
        );
        
        if (stopResult.success) {
            print("✓ Балансировщик остановлен успешно");
        } else {
            print("✗ Ошибка остановки балансировщика");
        }
        break;
        
    case 'enable':
        print("\nВключение балансировщика...");
        const enableResult = executeCommand(
            { setBalancerState: 1 },
            "Включение балансировщика"
        );
        
        if (enableResult.success) {
            print("✓ Балансировщик включен успешно");
        } else {
            print("✗ Ошибка включения балансировщика");
        }
        break;
        
    case 'disable':
        print("\nОтключение балансировщика...");
        const disableResult = executeCommand(
            { setBalancerState: 0 },
            "Отключение балансировщика"
        );
        
        if (disableResult.success) {
            print("✓ Балансировщик отключен успешно");
        } else {
            print("✗ Ошибка отключения балансировщика");
        }
        break;
        
    case 'status':
    default:
        print("\nПроверка состояния балансировщика...");
        
        // Проверка состояния балансировщика
        const stateResult = executeCommand(
            { getBalancerState: 1 },
            "Проверка состояния балансировщика"
        );
        
        if (stateResult.success) {
            print(`Состояние балансировщика: ${stateResult.result.enabled ? 'включен' : 'отключен'}`);
        } else {
            print("✗ Не удалось получить состояние балансировщика");
        }
        
        // Проверка работы балансировщика
        const runningResult = executeCommand(
            { isBalancerRunning: 1 },
            "Проверка работы балансировщика"
        );
        
        if (runningResult.success) {
            print(`Балансировщик работает: ${runningResult.result.inBalancerRound ? 'да' : 'нет'}`);
        } else {
            print("✗ Не удалось проверить работу балансировщика");
        }
        
        // Дополнительная информация о балансировщике
        try {
            const balancerStatus = db.getSiblingDB('config').getCollection('balancer').findOne({});
            if (balancerStatus) {
                print(`Активен: ${balancerStatus.active ? 'да' : 'нет'}`);
                print(`Окно балансировки: ${balancerStatus.window ? 'настроено' : 'не настроено'}`);
                if (balancerStatus.stopped) {
                    print(`Остановлен: ${balancerStatus.stopped}`);
                }
            }
        } catch (error) {
            print(`Дополнительная информация недоступна: ${error.message}`);
        }
        break;
}

print("\n=== Управление балансировщиком завершено ===");
print("Доступные команды:");
print("  start   - запустить балансировщик");
print("  stop    - остановить балансировщик");
print("  enable  - включить балансировщик");
print("  disable - отключить балансировщик");
print("  status  - проверить состояние (по умолчанию)");
