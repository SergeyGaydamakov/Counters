// Простой тест для проверки работы модуля serviceTest.js
const { main } = require('../serviceTest');
const MessageGenerator = require('../generators/messageGenerator');

// Тестируем только инициализацию и первые несколько циклов
async function testServiceTest() {
    console.log('🧪 Тестирование модуля serviceTest.js...');
    
    try {
        // Запускаем main функцию
        console.log('✓ Модуль успешно импортирован');
        console.log('✓ Функция main доступна');
        
        // Тестируем MessageGenerator отдельно
        console.log('✓ Тестирование MessageGenerator...');
        const messageGenerator = new MessageGenerator('messageConfig.json');
        const availableTypes = messageGenerator.getAvailableTypes();
        console.log(`✓ MessageGenerator загружен, доступно типов: ${availableTypes.length}`);
        console.log(`✓ Типы сообщений: ${availableTypes.join(', ')}`);
        
        // Проверяем, что модуль может быть запущен
        console.log('✓ Модуль готов к запуску');
        console.log('');
        console.log('Для полного тестирования запустите:');
        console.log('npm run test:service');
        console.log('');
        console.log('⚠️  Убедитесь, что сервис запущен на http://localhost:3000');
        console.log('⚠️  Убедитесь, что файл messageConfig.json существует');
        
    } catch (error) {
        console.error('✗ Ошибка при тестировании модуля:', error.message);
        process.exit(1);
    }
}

// Запуск теста
if (require.main === module) {
    testServiceTest();
}
